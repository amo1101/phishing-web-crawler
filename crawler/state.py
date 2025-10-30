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

CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  type TEXT NOT NULL,            -- LIVE_CREATE | WAYBACK_CREATE
  url TEXT NOT NULL,
  link TEXT,
  payload TEXT,
  status TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING | RUNNING | SUCCEEDED | FAILED
  priority INTEGER NOT NULL DEFAULT 100,
  attempts INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_url ON jobs(url);
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

    # --- JOB QUEUE HELPERS (used by jobqueue.py and scheduler) ---
    def enqueue_job_unique(self, job_type: str, url: str, priority: int = 100) -> int | None:
        """
        Insert a PENDING job if there is no job of the same url + type
        Return job id or None if skipped.
        """
        cur = self.conn.cursor()
        row = cur.execute(
            "SELECT id FROM jobs WHERE type=? AND url=? AND status='PENDING' LIMIT 1",
            (job_type, url)
        ).fetchone()
        if row:
            log.debug("Skip enqueue: existing PENDING type=%s url=%s", job_type, url)
            return None
        now = datetime.now(datetime.timezone.utc).isoformat()
        cur.execute(
            """INSERT INTO jobs(type,url,status,priority,created_at,updated_at)
               VALUES(?,?,?,?,?,?)""",
            (job_type, url, "PENDING", priority, now, now)
        )
        log.info("Enqueued job type=%s url=%s priority=%s", job_type, url, priority)
        return cur.lastrowid

    def fetch_next_pending(self, limit: int) -> list[dict]:
        rows = self.conn.execute(
            """SELECT id,type,url,payload,priority,attempts FROM jobs
               WHERE status='PENDING'
               ORDER BY priority ASC, created_at ASC
               LIMIT ?""", (limit,)
        ).fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r[0], "type": r[1], "url": r[2],
                "payload": json.loads(r[3] or "{}"),
                "priority": r[4], "attempts": r[5],
            })
        return out

    def mark_running(self, job_id: int):
        now = datetime.now(datetime.timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE jobs SET status='RUNNING', started_at=?, updated_at=? WHERE id=?",
            (now, now, job_id)
        )

    def mark_succeeded(self, job_id: int):
        now = datetime.now(datetime.timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE jobs SET status='SUCCEEDED', finished_at=?, updated_at=? WHERE id=?",
            (now, now, job_id)
        )

    def mark_failed(self, job_id: int, error: str, max_retries: int):
        cur = self.conn.cursor()
        now = datetime.now(datetime.timezone.utc).isoformat()
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

    def count_running_jobs(self) -> int:
        row = self.conn.execute("SELECT COUNT(*) FROM jobs WHERE status='RUNNING'").fetchone()
        return row[0] if row else 0

    def list_running_jobs(self) -> list[dict]:
        rows = self.conn.execute(
            "SELECT id,type,url FROM jobs WHERE status='RUNNING'"
        ).fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r[0], "type": r[1], "url": r[2]
            })
        return out

    def update_job_payload(self, job_id: int, payload: dict):
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            "UPDATE jobs SET payload=?, updated_at=? WHERE id=?",
            (json.dumps(payload or {}), now, job_id)
        )

    def job_running(self, url) -> bool:
        rows = self.conn.execute(
            "SELECT id FROM jobs WHERE url=? and status='RUNNING' LIMIT 1",
            (url,)
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

    def fetch_all_urls(self) -> list[dict]:
        rows = self.conn.execute(
            """SELECT url FROM jobs""",
        ).fetchall()
        out = []
        for r in rows:
            out.append(r[0])
        return out
