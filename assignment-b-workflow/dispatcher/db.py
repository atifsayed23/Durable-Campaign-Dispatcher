from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from typing import Any, Generator, Iterable, Optional


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id TEXT PRIMARY KEY,
    max_concurrent_calls INTEGER NOT NULL,
    webhook_secret TEXT
);

CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id),
    leads_file TEXT,
    window_start TEXT NOT NULL,
    window_end TEXT NOT NULL,
    window_days TEXT NOT NULL,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    no_answer_wait_min INTEGER NOT NULL DEFAULT 60,
    busy_wait_min INTEGER NOT NULL DEFAULT 60,
    failed_wait_min INTEGER NOT NULL DEFAULT 15,
    webhook_secret TEXT
);

CREATE TABLE IF NOT EXISTS global_dnd (
    phone TEXT PRIMARY KEY,
    source_call_sid TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS leads (
    campaign_id TEXT NOT NULL REFERENCES campaigns(campaign_id),
    lead_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL REFERENCES tenants(tenant_id),
    phone TEXT NOT NULL,
    customer_name TEXT,
    state TEXT NOT NULL DEFAULT 'WAITING',
    priority_tier INTEGER NOT NULL DEFAULT 3,
    next_eligible_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_call_sid TEXT,
    last_status TEXT,
    last_disposition TEXT,
    last_ended_at TEXT,
    callback_at TEXT,
    PRIMARY KEY (campaign_id, lead_id)
);

CREATE TABLE IF NOT EXISTS call_attempts (
    request_id TEXT PRIMARY KEY,
    call_sid TEXT UNIQUE,
    campaign_id TEXT NOT NULL,
    lead_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    phone TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt_no INTEGER NOT NULL,
    counted_attempt INTEGER NOT NULL DEFAULT 0,
    created_at_real REAL NOT NULL,
    created_at_sim TEXT NOT NULL,
    status TEXT,
    disposition TEXT,
    callback_at TEXT,
    started_at TEXT,
    ended_at TEXT,
    duration_sec INTEGER DEFAULT 0,
    error_detail TEXT,
    FOREIGN KEY (campaign_id, lead_id) REFERENCES leads(campaign_id, lead_id)
);

CREATE INDEX IF NOT EXISTS idx_leads_state_tier_next ON leads(state, priority_tier, next_eligible_at);
CREATE INDEX IF NOT EXISTS idx_leads_phone ON leads(phone);
CREATE INDEX IF NOT EXISTS idx_leads_tenant ON leads(tenant_id, state);
CREATE INDEX IF NOT EXISTS idx_call_attempts_state ON call_attempts(state);
CREATE INDEX IF NOT EXISTS idx_call_attempts_call_sid ON call_attempts(call_sid);
CREATE INDEX IF NOT EXISTS idx_call_attempts_lead ON call_attempts(campaign_id, lead_id);
CREATE INDEX IF NOT EXISTS idx_call_attempts_tenant ON call_attempts(tenant_id, state);
"""


class Database:
    """
    SQLite database wrapper with Write-Ahead Logging (WAL) and BEGIN IMMEDIATE transactions.
    """
    def __init__(self, db_path: str = "dispatcher_state.db"):
        self.db_path = db_path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(
            self.db_path,
            check_same_thread=False,
            isolation_level=None,  # Autocommit mode: explicit transaction control
        )
        self.conn.row_factory = sqlite3.Row
        self._apply_pragmas()
        self.init_schema()

    def _apply_pragmas(self) -> None:
        with self._lock:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA synchronous=NORMAL;")
            self.conn.execute("PRAGMA busy_timeout=10000;")
            self.conn.execute("PRAGMA foreign_keys=ON;")

    def init_schema(self) -> None:
        """Create tables and indexes if they do not exist."""
        with self._lock:
            self.conn.executescript(SCHEMA_SQL)

    @contextmanager
    def atomic(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Wrap operations in BEGIN IMMEDIATE ... COMMIT/ROLLBACK.
        Guarantees that write intent is locked immediately and serialized safely.
        """
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def execute(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Cursor:
        with self._lock:
            return self.conn.execute(sql, params)

    def executemany(self, sql: str, seq_of_params: Iterable[Iterable[Any]]) -> sqlite3.Cursor:
        with self._lock:
            return self.conn.executemany(sql, seq_of_params)

    def fetchone(self, sql: str, params: Iterable[Any] = ()) -> Optional[sqlite3.Row]:
        with self._lock:
            cursor = self.conn.execute(sql, params)
            return cursor.fetchone()

    def fetchall(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        with self._lock:
            cursor = self.conn.execute(sql, params)
            return cursor.fetchall()

    def close(self) -> None:
        with self._lock:
            self.conn.close()

