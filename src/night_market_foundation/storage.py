"""封装 SQLite 连接、建表和事务边界。"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SCHEMA = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS organizations (
    organization_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS actors (
    actor_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    role TEXT NOT NULL,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    active INTEGER NOT NULL CHECK(active IN (0, 1)),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sites (
    site_id TEXT PRIMARY KEY,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    name TEXT NOT NULL,
    timezone_name TEXT NOT NULL,
    version INTEGER NOT NULL CHECK(version >= 1),
    closed INTEGER NOT NULL DEFAULT 0 CHECK(closed IN (0, 1)),
    closed_at TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS domain_records (
    record_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    category TEXT NOT NULL,
    external_key TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    created_by TEXT NOT NULL REFERENCES actors(actor_id),
    created_at TEXT NOT NULL,
    UNIQUE(site_id, category, external_key)
);
CREATE TABLE IF NOT EXISTS request_receipts (
    request_id TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    actor_id TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    previous_hash TEXT NOT NULL,
    event_hash TEXT NOT NULL UNIQUE,
    occurred_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS party_grants (
    grant_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    actor_id TEXT NOT NULL REFERENCES actors(actor_id),
    party_id TEXT NOT NULL,
    party_role TEXT NOT NULL CHECK(party_role IN ('creator', 'stall', 'charity')),
    created_at TEXT NOT NULL,
    UNIQUE(site_id, actor_id),
    UNIQUE(site_id, party_id, party_role)
);
CREATE TABLE IF NOT EXISTS consignment_batches (
    batch_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    creator_id TEXT NOT NULL,
    stall_id TEXT NOT NULL,
    work_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK(quantity >= 0),
    unit_price_minor INTEGER NOT NULL CHECK(unit_price_minor >= 0),
    creator_bp INTEGER NOT NULL CHECK(creator_bp BETWEEN 0 AND 10000),
    stall_bp INTEGER NOT NULL CHECK(stall_bp BETWEEN 0 AND 10000),
    charity_bp INTEGER NOT NULL CHECK(charity_bp BETWEEN 0 AND 10000),
    charity_party_id TEXT NOT NULL,
    rule_hash TEXT NOT NULL,
    confirmed_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    CHECK(creator_bp + stall_bp + charity_bp = 10000),
    UNIQUE(site_id, creator_id, stall_id, work_id)
);
CREATE TABLE IF NOT EXISTS quantity_events (
    event_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES consignment_batches(batch_id),
    site_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK(event_type IN
        ('sale', 'return', 'loss', 'transfer_out', 'transfer_in', 'adjustment')),
    quantity INTEGER NOT NULL CHECK(quantity <> 0),
    request_id TEXT NOT NULL UNIQUE,
    terminal_id TEXT,
    terminal_seq INTEGER CHECK(terminal_seq IS NULL OR terminal_seq >= 1),
    gap_before INTEGER NOT NULL DEFAULT 0 CHECK(gap_before IN (0, 1)),
    event_hash TEXT NOT NULL UNIQUE,
    transfer_id TEXT,
    dispute_id TEXT,
    adjust_mode TEXT CHECK(adjust_mode IS NULL OR adjust_mode IN ('stock', 'sold')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    CHECK(event_type = 'adjustment' OR quantity > 0),
    UNIQUE(terminal_id, terminal_seq)
);
CREATE TABLE IF NOT EXISTS transfers (
    transfer_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL,
    from_batch_id TEXT NOT NULL REFERENCES consignment_batches(batch_id),
    to_batch_id TEXT NOT NULL REFERENCES consignment_batches(batch_id),
    quantity INTEGER NOT NULL CHECK(quantity > 0),
    status TEXT NOT NULL CHECK(status IN ('pending', 'effective', 'rejected', 'cancelled')),
    source_ack_by TEXT NOT NULL,
    source_ack_at TEXT NOT NULL,
    dest_ack_by TEXT,
    dest_ack_at TEXT,
    request_id TEXT NOT NULL UNIQUE,
    terminal_id TEXT,
    terminal_seq INTEGER,
    gap_before INTEGER NOT NULL DEFAULT 0 CHECK(gap_before IN (0, 1)),
    payload_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS settlements (
    settlement_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    version INTEGER NOT NULL CHECK(version >= 1),
    supersedes_id TEXT,
    rule_snapshot_hash TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    watermark_rowid INTEGER NOT NULL,
    pending_transfer_ids_json TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(site_id, version)
);
CREATE TABLE IF NOT EXISTS settlement_lines (
    line_id TEXT PRIMARY KEY,
    settlement_id TEXT NOT NULL REFERENCES settlements(settlement_id),
    batch_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    UNIQUE(settlement_id, batch_id)
);
CREATE TABLE IF NOT EXISTS settlement_confirmations (
    settlement_id TEXT NOT NULL REFERENCES settlements(settlement_id),
    party_id TEXT NOT NULL,
    party_role TEXT NOT NULL,
    confirmed_by TEXT NOT NULL,
    rule_snapshot_hash TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(settlement_id, party_id)
);
CREATE TABLE IF NOT EXISTS disputes (
    dispute_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL,
    settlement_id TEXT NOT NULL,
    party_id TEXT NOT NULL,
    party_role TEXT NOT NULL,
    reason TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    evidence_hash TEXT NOT NULL,
    raised_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dispute_batches (
    dispute_id TEXT NOT NULL REFERENCES disputes(dispute_id),
    batch_id TEXT NOT NULL,
    PRIMARY KEY(dispute_id, batch_id)
);
CREATE TABLE IF NOT EXISTS dispute_decisions (
    decision_id TEXT PRIMARY KEY,
    dispute_id TEXT NOT NULL REFERENCES disputes(dispute_id),
    action TEXT NOT NULL CHECK(action IN ('release', 'adjust', 'reject')),
    note TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    evidence_hash TEXT NOT NULL,
    decided_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_freezes (
    settlement_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    dispute_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(settlement_id, batch_id, dispute_id)
);
"""


class Database:
    """管理 SQLite 数据库并为服务提供短事务。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self.connection = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.connection.execute("PRAGMA busy_timeout = 5000")
        self.connection.executescript(SCHEMA)

    @contextmanager
    def transaction(self, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        """在异常时回滚，在成功时提交。"""

        self.connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
        try:
            yield self.connection
        except Exception:
            self.connection.rollback()
            raise
        else:
            self.connection.commit()

    def close(self) -> None:
        """关闭底层连接。"""

        self.connection.close()
