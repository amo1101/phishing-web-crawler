from __future__ import annotations
import json
import sqlite3
from pathlib import Path
from typing import Optional, Tuple, List, Dict
from datetime import datetime, timezone
import threading
import logging

log = logging.getLogger(__name__)

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS domains (
  domain TEXT PRIMARY KEY,
  last_live_status TEXT,
  last_seen TIMESTAMP,
  last_launch TIMESTAMP,
  job_kind TEXT,
  wayback_timestamps TEXT
);

CREATE TABLE IF NOT EXISTS urls (
  url TEXT PRIMARY KEY,
  domain TEXT NOT NULL,
  first_seen TIMESTAMP,
  last_seen TIMESTAMP
);

CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,            -- LIVE_CREATE | LIVE_RELAUNCH | WAYBACK_CREATE
  domain TEXT NOT NULL,
  payload TEXT,                  -- JSON (e.g., {"seeds":[...]} or {"timestamps":[...], "job_names":[...]})
  status TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING | RUNNING | SUCCEEDED | FAILED | SKIPPED
  priority INTEGER NOT NULL DEFAULT 100,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  started_at TIMESTAMP,
  finished_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_urls_domain ON urls(domain);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_domain ON jobs(domain);
"""

class State:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        # Ensure schema exists in the creating thread
        conn = self._conn()
        self._init_db(conn)
        log.info("SQLite state initialized at %s", self.db_path)

    def _conn(self) -> sqlite3.Connection:
        """Return a per-thread SQLite connection (create if missing)."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(
                self.db_path,
                isolation_level=None,        # autocommit mode
                check_same_thread=False,     # allow use in this thread (distinct conn per thread)
                detect_types=sqlite3.PARSE_DECLTYPES,
            )
            # Pragmas for concurrency/consistency
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous = NORMAL;")
            self._local.conn = conn
            # Optionally, ensure schema (idempotent)
            self._init_db(conn)
        return conn

    @property
    def conn(self) -> sqlite3.Connection:
        """Compatibility: expose the current thread's connection."""
        return self._conn()

    def close(self):
        """Close the current thread's connection, if any."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            try:
                conn.close()
            finally:
                self._local.conn = None

    def _init_db(self, conn: sqlite3.Connection):
        cur = conn.cursor()
        cur.executescript(SCHEMA)

    # --- meta helpers ---
    def get_meta(self, key: str) -> Optional[str]:
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        self.conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))

    def get_last_full_run(self) -> Optional[datetime]:
        v = self.get_meta("last_full_run")
        return datetime.fromisoformat(v) if v else None

    def set_last_full_run(self, dt: datetime) -> None:
        self.set_meta("last_full_run", dt.replace(tzinfo=timezone.utc).isoformat())

    def get_last_incremental_run(self) -> Optional[datetime]:
        v = self.get_meta("last_incremental_run")
        return datetime.fromisoformat(v) if v else None

    def set_last_incremental_run(self, dt: datetime) -> None:
        self.set_meta("last_incremental_run", dt.replace(tzinfo=timezone.utc).isoformat())

    # --- urls/domains ---
    def upsert_url(self, url: str, domain: str, seen_at: datetime):
        cur = self.conn.cursor()
        cur.execute("SELECT url FROM urls WHERE url=?", (url,))
        if cur.fetchone():
            self.conn.execute("UPDATE urls SET last_seen=? WHERE url=?", (seen_at.isoformat(), url))
        else:
            self.conn.execute("INSERT INTO urls(url, domain, first_seen, last_seen) VALUES(?,?,?,?)",
                              (url, domain, seen_at.isoformat(), seen_at.isoformat()))
        # update domain last seen
        self.conn.execute("INSERT INTO domains(domain,last_seen) VALUES(?,?) ON CONFLICT(domain) DO UPDATE SET last_seen=excluded.last_seen",
                          (domain, seen_at.isoformat()))
        log.debug("Upsert url=%s domain=%s seen_at=%s", url, domain, seen_at.isoformat())

    def set_domain_status(self, domain: str, status: str):
        self.conn.execute("INSERT INTO domains(domain,last_live_status) VALUES(?,?) ON CONFLICT(domain) DO UPDATE SET last_live_status=excluded.last_live_status",
                          (domain, status))

    def get_domains_due_for_heritrix(self, cadence_days: int) -> List[str]:
        # select domains where last_heritrix_launch older than cadence_days (or NULL)
        q = """
        SELECT domain FROM domains
        WHERE last_seen IS NOT NULL
          AND job_kind IN ('LIVE_CREATE', 'LIVE_RELAUNCH')
          AND (last_launch IS NOT NULL
               AND julianday('now') - julianday(last_launch) >= ?)
        """
        rows = self.conn.execute(q, (cadence_days,)).fetchall()
        return [r[0] for r in rows]

    def mark_launch(self, domain: str, job_type: str, when: Optional[datetime] = None):
        when = when or datetime.utcnow()
        self.conn.execute("UPDATE domains SET last_launch=? job_kind=? WHERE domain=?", (when.isoformat(), job_type, domain))

    def record_wayback_timestamps(self, domain: str, stamps: List[str]):
        stamps_csv = ",".join(stamps)
        self.conn.execute("INSERT INTO domains(domain, wayback_timestamps) VALUES(?,?) ON CONFLICT(domain) DO UPDATE SET wayback_timestamps=?",
                          (domain, stamps_csv, stamps_csv))

    def list_all_domains(self) -> List[str]:
        rows = self.conn.execute("SELECT domain FROM domains").fetchall()
        return [r[0] for r in rows]

    # --- JOB QUEUE HELPERS (used by jobqueue.py and scheduler) ---
    def enqueue_job_unique(self, job_type: str, domain: str, payload: dict, priority: int = 100) -> int | None:
        """
        Insert a PENDING job if there is no other PENDING job of the same type+domain.
        Return job id or None if skipped.
        """
        cur = self.conn.cursor()
        row = cur.execute(
            "SELECT id FROM jobs WHERE type=? AND domain=? AND status='PENDING' LIMIT 1",
            (job_type, domain)
        ).fetchone()
        if row:
            log.debug("Skip enqueue: existing PENDING type=%s domain=%s", job_type, domain)
            return None
        now = datetime.utcnow().isoformat()
        cur.execute(
            """INSERT INTO jobs(type,domain,payload,status,priority,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?)""",
            (job_type, domain, json.dumps(payload or {}), "PENDING", priority, now, now)
        )
        log.info("Enqueued job type=%s domain=%s priority=%s", job_type, domain, priority)
        return cur.lastrowid

    def fetch_next_pending(self, limit: int) -> list[dict]:
        rows = self.conn.execute(
            """SELECT id,type,domain,payload,priority,attempts FROM jobs
               WHERE status='PENDING'
               ORDER BY priority ASC, created_at ASC
               LIMIT ?""", (limit,)
        ).fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r[0], "type": r[1], "domain": r[2],
                "payload": json.loads(r[3] or "{}"),
                "priority": r[4], "attempts": r[5],
            })
        return out

    def mark_running(self, job_id: int):
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            "UPDATE jobs SET status='RUNNING', started_at=?, updated_at=? WHERE id=?",
            (now, now, job_id)
        )

    def mark_succeeded(self, job_id: int):
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            "UPDATE jobs SET status='SUCCEEDED', finished_at=?, updated_at=? WHERE id=?",
            (now, now, job_id)
        )

    def mark_failed(self, job_id: int, error: str, max_retries: int):
        cur = self.conn.cursor()
        now = datetime.utcnow().isoformat()
        row = cur.execute("SELECT attempts FROM jobs WHERE id=?", (job_id,)).fetchone()
        attempts = (row[0] if row else 0) + 1
        if attempts >= max_retries:
            self.conn.execute(
                "UPDATE jobs SET status='FAILED', attempts=?, last_error=?, finished_at=?, updated_at=? WHERE id=?",
                (attempts, error[:2000], now, now, job_id)
            )
        else:
            # Put back to PENDING for retry
            self.conn.execute(
                "UPDATE jobs SET status='PENDING', attempts=?, last_error=?, updated_at=? WHERE id=?",
                (attempts, error[:2000], now, job_id)
            )

    def mark_skipped(self, job_id: int, reason: str = ""):
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            "UPDATE jobs SET status='SKIPPED', last_error=?, finished_at=?, updated_at=? WHERE id=?",
            (reason[:2000], now, now, job_id)
        )

    def count_running_jobs(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM jobs WHERE status='RUNNING'").fetchone()
        return row[0] if row else 0

    def list_running_jobs(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT id,type,domain,payload FROM jobs WHERE status='RUNNING'"
        ).fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r[0], "type": r[1], "domain": r[2], "payload": json.loads(r[3] or "{}")
            })
        return out

    def update_job_payload(self, job_id: int, payload: dict):
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            "UPDATE jobs SET payload=?, updated_at=? WHERE id=?",
            (json.dumps(payload or {}), now, job_id)
        )

    def job_running(self, domain) -> bool:
        rows = self.conn.execute(
            "SELECT id FROM jobs WHERE domain=? and status='RUNNING' LIMIT 1",
            (domain,)
        ).fetchall()
        return len(rows) > 0
    
    def wayback_job_finished(self, domain) -> bool:
        rows = self.conn.execute(
            "SELECT id FROM jobs WHERE domain=? and type='WAYBACK_CREATE' and status='SUCCEEDED' LIMIT 1",
            (domain,)
        ).fetchall()
        return len(rows) > 0
    
    def wayback_latest_job_status(self, domain) -> str:
        r = self.conn.execute(
            "SELECT payload, status FROM jobs WHERE domain=? and type='WAYBACK_CREATE' ORDER BY created_at DESC LIMIT 1",
            (domain,)
        ).fetchone()
        return {"payload": json.loads(r[0] or "{}"), "status": r[1]} if r else None
