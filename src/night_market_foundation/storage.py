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
CREATE TABLE IF NOT EXISTS settlement_rules (
    rule_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    name TEXT NOT NULL,
    creator_bp INTEGER NOT NULL CHECK(creator_bp >= 0),
    stall_bp INTEGER NOT NULL CHECK(stall_bp >= 0),
    charity_bp INTEGER NOT NULL CHECK(charity_bp >= 0),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS parties (
    party_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    kind TEXT NOT NULL CHECK(kind IN ('creator', 'stall', 'charity')),
    name TEXT NOT NULL,
    organization_id TEXT NOT NULL REFERENCES organizations(organization_id),
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS consignment_batches (
    batch_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    creator_id TEXT NOT NULL REFERENCES parties(party_id),
    stall_id TEXT NOT NULL REFERENCES parties(party_id),
    item_name TEXT NOT NULL,
    unit_price_fen INTEGER NOT NULL CHECK(unit_price_fen > 0),
    consigned_qty INTEGER NOT NULL CHECK(consigned_qty >= 0),
    rule_id TEXT NOT NULL REFERENCES settlement_rules(rule_id),
    rule_snapshot_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('open', 'closed')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_events (
    event_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES consignment_batches(batch_id),
    kind TEXT NOT NULL CHECK(kind IN ('sale', 'return', 'loss', 'transfer_out', 'transfer_in', 'adjustment')),
    quantity INTEGER NOT NULL,
    unit_price_fen INTEGER,
    terminal_id TEXT,
    terminal_seq INTEGER,
    payload_hash TEXT NOT NULL,
    transfer_id TEXT,
    decision_id TEXT,
    note TEXT,
    settled_draft_id TEXT,
    compensated_draft_id TEXT,
    recorded_by TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(terminal_id, terminal_seq)
);
CREATE INDEX IF NOT EXISTS idx_batch_events_batch ON batch_events(batch_id);
CREATE TABLE IF NOT EXISTS sequence_forks (
    fork_id TEXT PRIMARY KEY,
    terminal_id TEXT NOT NULL,
    terminal_seq INTEGER NOT NULL,
    existing_event_id TEXT NOT NULL,
    attempted_payload_hash TEXT NOT NULL,
    attempted_by TEXT NOT NULL,
    detected_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS transfers (
    transfer_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    from_batch_id TEXT NOT NULL REFERENCES consignment_batches(batch_id),
    to_batch_id TEXT NOT NULL REFERENCES consignment_batches(batch_id),
    quantity INTEGER NOT NULL CHECK(quantity > 0),
    status TEXT NOT NULL CHECK(status IN ('awaiting_receipts', 'applied')),
    out_receipt_by TEXT,
    out_receipt_at TEXT,
    in_receipt_by TEXT,
    in_receipt_at TEXT,
    applied_at TEXT,
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS settlement_drafts (
    draft_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    snapshot_json TEXT NOT NULL,
    snapshot_hash TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('open', 'completed')),
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS draft_lines (
    line_id TEXT PRIMARY KEY,
    draft_id TEXT NOT NULL REFERENCES settlement_drafts(draft_id),
    batch_id TEXT NOT NULL REFERENCES consignment_batches(batch_id),
    creator_id TEXT NOT NULL,
    stall_id TEXT NOT NULL,
    sold_qty INTEGER NOT NULL,
    gross_fen INTEGER NOT NULL,
    creator_fen INTEGER NOT NULL,
    stall_fen INTEGER NOT NULL,
    charity_fen INTEGER NOT NULL,
    event_ids_json TEXT NOT NULL,
    compensation_event_ids_json TEXT NOT NULL,
    UNIQUE(draft_id, batch_id)
);
CREATE TABLE IF NOT EXISTS draft_confirmations (
    draft_id TEXT NOT NULL REFERENCES settlement_drafts(draft_id),
    party_id TEXT NOT NULL REFERENCES parties(party_id),
    snapshot_hash TEXT NOT NULL,
    confirmed_by TEXT NOT NULL,
    confirmed_at TEXT NOT NULL,
    PRIMARY KEY (draft_id, party_id)
);
CREATE TABLE IF NOT EXISTS disputes (
    dispute_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    batch_id TEXT NOT NULL REFERENCES consignment_batches(batch_id),
    draft_id TEXT,
    party_id TEXT NOT NULL REFERENCES parties(party_id),
    evidence TEXT NOT NULL,
    expected_qty INTEGER,
    status TEXT NOT NULL CHECK(status IN ('open', 'resolved')),
    raised_by TEXT NOT NULL,
    resolved_decision_id TEXT,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS freezes (
    freeze_id TEXT PRIMARY KEY,
    dispute_id TEXT NOT NULL REFERENCES disputes(dispute_id),
    batch_id TEXT NOT NULL REFERENCES consignment_batches(batch_id),
    amount_fen INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('frozen', 'released')),
    basis_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    released_at TEXT,
    release_decision_id TEXT
);
CREATE INDEX IF NOT EXISTS idx_freezes_batch ON freezes(batch_id, status);
CREATE TABLE IF NOT EXISTS decisions (
    decision_id TEXT PRIMARY KEY,
    dispute_id TEXT NOT NULL REFERENCES disputes(dispute_id),
    action TEXT NOT NULL CHECK(action IN ('release', 'void_event', 'adjust_quantity')),
    target_event_id TEXT,
    adjust_qty INTEGER,
    note TEXT NOT NULL,
    decided_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS event_voids (
    void_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE REFERENCES batch_events(event_id),
    decision_id TEXT NOT NULL REFERENCES decisions(decision_id),
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL
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
