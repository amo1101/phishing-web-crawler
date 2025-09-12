from __future__ import annotations
import sqlite3
from pathlib import Path
from typing import Optional, Tuple, List, Dict
from datetime import datetime, timezone

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT
);

CREATE TABLE IF NOT EXISTS domains (
  domain TEXT PRIMARY KEY,
  last_live_status TEXT,          -- 'live' | 'dead' | NULL
  last_seen TIMESTAMP,            -- when we saw it in CSV
  last_heritrix_launch TIMESTAMP, -- last time we launched a crawl for this domain
  job_kind TEXT,                  -- 'live' or 'wayback' (most recent mode used)
  wayback_timestamps TEXT         -- comma-separated list of timestamps last used
);

CREATE TABLE IF NOT EXISTS urls (
  url TEXT PRIMARY KEY,
  domain TEXT NOT NULL,
  first_seen TIMESTAMP,
  last_seen TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_urls_domain ON urls(domain);
"""

class State:
    def __init__(self, db_path: str):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, isolation_level=None)
        self.conn.execute("PRAGMA foreign_keys = ON;")
        self._init_db()

    def _init_db(self):
        cur = self.conn.cursor()
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

    def set_domain_status(self, domain: str, status: str):
        self.conn.execute("INSERT INTO domains(domain,last_live_status) VALUES(?,?) ON CONFLICT(domain) DO UPDATE SET last_live_status=excluded.last_live_status",
                          (domain, status))

    def get_domains_due_for_heritrix(self, cadence_days: int) -> List[str]:
        # select domains where last_heritrix_launch older than cadence_days (or NULL)
        q = """
        SELECT domain FROM domains
        WHERE last_seen IS NOT NULL
          AND (last_heritrix_launch IS NULL
               OR julianday('now') - julianday(last_heritrix_launch) >= ?)
        """
        rows = self.conn.execute(q, (cadence_days,)).fetchall()
        return [r[0] for r in rows]

    def mark_heritrix_launch(self, domain: str, when: Optional[datetime] = None):
        when = when or datetime.utcnow()
        self.conn.execute("UPDATE domains SET last_heritrix_launch=? WHERE domain=?", (when.isoformat(), domain))

    def record_wayback_timestamps(self, domain: str, stamps: List[str]):
        stamps_csv = ",".join(stamps)
        self.conn.execute("INSERT INTO domains(domain, wayback_timestamps) VALUES(?,?) ON CONFLICT(domain) DO UPDATE SET wayback_timestamps=?",
                          (domain, stamps_csv, stamps_csv))

    def list_all_domains(self) -> List[str]:
        rows = self.conn.execute("SELECT domain FROM domains").fetchall()
        return [r[0] for r in rows]
