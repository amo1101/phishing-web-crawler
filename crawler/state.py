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
  job_name TEXT,
  type TEXT NOT NULL,
  url TEXT NOT NULL,
  nca_id INTEGER NOT NULL,
  validation_date DATE,
  link TEXT,
  priority INTEGER NOT NULL DEFAULT 100,
  attempts INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING | RUNNING | SUCCEEDED | FAILED
  crawl_count INTEGER NOT NULL DEFAULT 0,
  last_crawl_file_count INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS ncas (
  nca_id INTEGER PRIMARY KEY,
  nca_jurisdiction TEXT NOT NULL,
  nca_name TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_url ON jobs(url);
"""

class State:
    """SQLite-based state management."""
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
        """Get a meta value by key."""
        row = self.conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key: str, value: str) -> None:
        """Set a meta value by key."""
        self.conn.execute("INSERT INTO meta(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))

    def get_last_full_run(self) -> Optional[datetime]:
        """Get the timestamp of the last full run."""
        v = self.get_meta("last_full_run")
        return datetime.fromisoformat(v) if v else None

    def set_last_full_run(self, dt: datetime) -> None:
        """Set the timestamp of the last full run."""
        self.set_meta("last_full_run", dt.replace(tzinfo=timezone.utc).isoformat())

    def get_last_incremental_run(self) -> Optional[datetime]:
        """Get the timestamp of the last incremental run."""
        v = self.get_meta("last_incremental_run")
        return datetime.fromisoformat(v) if v else None

    def set_last_incremental_run(self, dt: datetime) -> None:
        """Set the timestamp of the last incremental run."""
        self.set_meta("last_incremental_run", dt.replace(tzinfo=timezone.utc).isoformat())

    def add_history_job(self,
                        job_name: str,
                        job_type: str,
                        url: str,
                        nca_id: int,
                        validation_date: str,
                        status: str,
                        crawl_count: int,
                        file_count: int,
                        link: str="") -> int:
        """Add a history job record."""
        now = datetime.now(timezone.utc).isoformat()
        cur = self.conn.cursor()
        cur.execute(
            """INSERT INTO jobs(job_name,type,url,nca_id,validation_date,link,status,crawl_count,last_crawl_file_count,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (job_name, job_type, url, nca_id, validation_date, link, status, crawl_count, file_count, now, now)
        )
        return cur.lastrowid

    def enqueue_job_unique(self, job_type: str, url: str, nca_id: int, validation_date: str, priority: int = 100) -> int | None:
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
        now = datetime.now(timezone.utc).isoformat()
        cur.execute(
            """INSERT INTO jobs(type,url,nca_id,validation_date,status,priority,created_at,updated_at)
               VALUES(?,?,?,?,?,?,?,?)""",
            (job_type, url, nca_id, validation_date, "PENDING", priority, now, now)
        )
        log.info("Enqueued job type=%s url=%s priority=%s", job_type, url, priority)
        return cur.lastrowid

    def fetch_next_pending(self, job_type: str, limit: int) -> list[dict]:
        """ Fetch next PENDING jobs up to limit, ordered by priority and created_at."""
        rows = self.conn.execute(
            """SELECT id,type,url,nca_id,validation_date FROM jobs
               WHERE status='PENDING' and type=?
               ORDER BY priority ASC, created_at ASC
               LIMIT ?""", (job_type, limit)
        ).fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r[0], "type": r[1], "url": r[2], "nca_id": r[3], "validation_date": r[4]
            })
        return out

    def mark_running(self, job_id: int):
        """Mark a job as RUNNING."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE jobs SET status='RUNNING', updated_at=? WHERE id=?",
            (now, job_id)
        )

    def mark_finished(self, job_id: int, crawl_count: int, file_count: int):
        """Mark a job as FINISHED."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE jobs SET status='FINISHED',crawl_count=?,last_crawl_file_count=?,updated_at=? WHERE id=?",
            (crawl_count, file_count, now, job_id)
        )

    def mark_failed(self, job_id: int, max_retries: int=0):
        """Mark a job as FAILED or re-PENDING for retry."""
        cur = self.conn.cursor()
        now = datetime.now(timezone.utc).isoformat()
        row = cur.execute("SELECT attempts FROM jobs WHERE id=?", (job_id,)).fetchone()
        attempts = (row[0] if row else 0) + 1
        if attempts >= max_retries:
            self.conn.execute(
                "UPDATE jobs SET status='FAILED', attempts=?, updated_at=? WHERE id=?",
                (attempts, now, job_id)
            )
        else:
            # Put back to PENDING for retry
            self.conn.execute(
                "UPDATE jobs SET status='PENDING', attempts=?, updated_at=? WHERE id=?",
                (attempts, now, job_id)
            )

    def count_running_jobs(self, job_type: str) -> int:
        """Count the number of RUNNING jobs."""
        row = self.conn.execute("SELECT COUNT(*) FROM jobs WHERE status='RUNNING' and type=?", (job_type,)).fetchone()
        return row[0] if row else 0

    def list_running_jobs(self) -> list[dict]:
        """List all RUNNING jobs."""
        rows = self.conn.execute(
            "SELECT id,type,job_name,url FROM jobs WHERE status='RUNNING'"
        ).fetchall()
        out = []
        for r in rows:
            out.append({
                "id": r[0], "type": r[1], "job_name": r[2], "url": r[3]
            })
        return out

    def update_job_name(self, job_id: int, job_name: str):
        """Update the job_name of a job."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE jobs SET job_name=?, updated_at=? WHERE id=?",
            (job_name, now, job_id)
        )

    def fetch_all_urls(self) -> list[dict]:
        """Fetch all URLs in the jobs table."""
        rows = self.conn.execute(
            """SELECT url FROM jobs""",
        ).fetchall()
        out = []
        for r in rows:
            out.append(r[0])
        return out

    def add_nca(self, nca_id: int, nca_jurisdiction: str, nca_name: str):
        """Add a national component authority."""
        cur = self.conn.cursor()
        row = cur.execute(
            "SELECT nca_id FROM ncas WHERE nca_id=? LIMIT 1",
            (nca_id,)
        ).fetchone()
        if row:
            return
        cur.execute(
            """INSERT INTO ncas(nca_id, nca_jurisdiction, nca_name)
               VALUES(?,?,?)""",
            (nca_id, nca_jurisdiction, nca_name)
        )

    def get_filtered_jobs(self, page: int = 1, per_page: int = 20,
                     job_type:str = None, status: str = None, jurisdiction: str = None,
                     date_from: str = None, date_to: str = None) -> Tuple[List[Dict], int]:
        """
        Get filtered and paginated jobs joined with NCA data.
        Returns tuple of (jobs list, total count)
        """
        query = """
            SELECT j.url, j.type AS job_type, j.link, j.status,
                   CAST(j.validation_date AS TEXT) AS validation_date, j.crawl_count, j.last_crawl_file_count,
                   CAST(j.created_at AS TEXT) AS created_at,
                   CAST(j.updated_at AS TEXT) AS last_update,
                   n.nca_jurisdiction, n.nca_name
            FROM jobs j
            LEFT JOIN ncas n ON j.nca_id = n.nca_id
            WHERE 1=1
        """
        params = []
        
        # Add filters
        if status:
            query += " AND j.status = ?"
            params.append(status)
        if job_type:
            query += " AND j.type = ?"
            params.append(job_type)
        if jurisdiction:
            query += " AND n.nca_jurisdiction = ?"
            params.append(jurisdiction)
        if date_from:
            query += " AND j.validation_date >= ?"
            params.append(date_from)
        if date_to:
            query += " AND j.validation_date <= ?"
            params.append(date_to)

        # Get total count
        count_query = f"SELECT COUNT(*) FROM ({query}) AS t"
        total = self.conn.execute(count_query, params).fetchone()[0]

        # Add pagination
        query += " ORDER BY j.created_at DESC LIMIT ? OFFSET ?"
        params.extend([per_page, (page - 1) * per_page])

        rows = self.conn.execute(query, params).fetchall()
        jobs = []
        for row in rows:
            jobs.append({
                "url": row[0],
                "type": row[1], 
                "link": row[2],
                "status": row[3],
                "validation_date": row[4],
                "crawl_count": row[5],
                "file_count": row[6],
                "created_at": row[7],
                "last_update": row[8],
                "jurisdiction": row[9],
                "nca_name": row[10]
            })
        return jobs, total

    def get_jurisdictions(self) -> List[str]:
        """Get list of unique NCA jurisdictions"""
        rows = self.conn.execute("SELECT DISTINCT nca_jurisdiction FROM ncas ORDER BY nca_jurisdiction").fetchall()
        return [r[0] for r in rows]
