"""SQLite 持久化：关系表 + 只增事件日志。

所有写操作在单个 BEGIN IMMEDIATE 事务内完成，并先写事件日志再改状态，
保证争议发生时可以按 seq 还原任意名额的完整生命周期。
"""
from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

SCHEMA = """
CREATE TABLE IF NOT EXISTS races (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    status     TEXT NOT NULL DEFAULT 'open',          -- open | lottery_published
    created_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS categories (
    id         TEXT PRIMARY KEY,
    race_id    TEXT NOT NULL REFERENCES races(id),
    name       TEXT NOT NULL,
    fee_fen    INTEGER NOT NULL DEFAULT 0,
    quota      INTEGER NOT NULL DEFAULT 0,            -- 总容量上限
    released   INTEGER NOT NULL DEFAULT 0,            -- 已分批放出的名额数
    status     TEXT NOT NULL DEFAULT 'registrations_open',
    created_by TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    UNIQUE(race_id, name)
);

CREATE TABLE IF NOT EXISTS slots (
    id              TEXT PRIMARY KEY,
    category_id     TEXT NOT NULL REFERENCES categories(id),
    seq             INTEGER NOT NULL,                 -- 组内确定顺序，从 1 开始
    status          TEXT NOT NULL DEFAULT 'free',     -- free | held | confirmed
    registration_id TEXT,
    UNIQUE(category_id, seq)
);

CREATE TABLE IF NOT EXISTS registrations (
    id           TEXT PRIMARY KEY,
    race_id      TEXT NOT NULL REFERENCES races(id),
    category_id  TEXT NOT NULL REFERENCES categories(id),
    id_type      TEXT NOT NULL,                       -- 证件类型
    id_no        TEXT NOT NULL,                       -- 证件号
    person_name  TEXT NOT NULL,
    status       TEXT NOT NULL,                       -- 见 actions.REGISTRATION_STATES
    slot_id      TEXT,
    waitlist_seq INTEGER,
    fee_fen      INTEGER NOT NULL DEFAULT 0,
    version      INTEGER NOT NULL DEFAULT 1,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
-- 同一证件在同一赛事内只能有一条“有效”报名；终态记录不阻挡重新报名
CREATE UNIQUE INDEX IF NOT EXISTS idx_reg_active_doc
    ON registrations(race_id, id_type, id_no)
    WHERE status NOT IN ('refunded', 'withdrawn', 'cancelled', 'lot_out');
CREATE INDEX IF NOT EXISTS idx_reg_slot ON registrations(slot_id);
CREATE INDEX IF NOT EXISTS idx_reg_wait
    ON registrations(category_id, waitlist_seq);

CREATE TABLE IF NOT EXISTS payments (
    id              TEXT PRIMARY KEY,
    notification_id TEXT NOT NULL UNIQUE,             -- 支付网关流水号（重送识别）
    registration_id TEXT NOT NULL REFERENCES registrations(id),
    amount_fen      INTEGER NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS refunds (
    id              TEXT PRIMARY KEY,
    notification_id TEXT NOT NULL UNIQUE,
    registration_id TEXT NOT NULL REFERENCES registrations(id),
    amount_fen      INTEGER NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS grants (
    staff_id    TEXT NOT NULL,
    category_id TEXT NOT NULL REFERENCES categories(id),
    granted_by  TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (staff_id, category_id)
);

-- 只增事件日志：争议还原的唯一事实来源
CREATE TABLE IF NOT EXISTS events (
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL UNIQUE,
    event_type      TEXT NOT NULL,
    race_id         TEXT,
    category_id     TEXT,
    slot_id         TEXT,
    registration_id TEXT,
    actor           TEXT NOT NULL,
    payload         TEXT NOT NULL DEFAULT '{}',
    created_at      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_slot ON events(slot_id);
CREATE INDEX IF NOT EXISTS idx_events_reg ON events(registration_id);
CREATE INDEX IF NOT EXISTS idx_events_cat ON events(category_id);

-- 命令幂等表：同一 request_id 永远返回首次结果
CREATE TABLE IF NOT EXISTS idempotency (
    request_id TEXT PRIMARY KEY,
    action     TEXT NOT NULL,
    accepted   INTEGER NOT NULL,
    state      TEXT NOT NULL,
    message    TEXT NOT NULL,
    data       TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, path: str = ":memory:") -> None:
        self.path = path
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        """串行化的写事务：BEGIN IMMEDIATE 一拿到锁即确定写入顺序。"""
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def close(self) -> None:
        self.conn.close()

    # ---- 事件日志 ----
    def append_event(
        self,
        tx: sqlite3.Connection,
        event_type: str,
        *,
        actor: str,
        race_id: str | None = None,
        category_id: str | None = None,
        slot_id: str | None = None,
        registration_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> int:
        cur = tx.execute(
            """
            INSERT INTO events (event_id, event_type, race_id, category_id, slot_id,
                                registration_id, actor, payload, created_at)
            VALUES (lower(hex(randomblob(16))), ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_type,
                race_id,
                category_id,
                slot_id,
                registration_id,
                actor,
                json.dumps(payload or {}, ensure_ascii=False, sort_keys=True),
                utcnow(),
            ),
        )
        return int(cur.lastrowid)

    def slot_timeline(self, slot_id: str) -> dict[str, Any]:
        slot = self.conn.execute(
            """
            SELECT s.*, c.race_id, c.name AS category_name
            FROM slots s JOIN categories c ON c.id = s.category_id
            WHERE s.id = ?
            """,
            (slot_id,),
        ).fetchone()
        if slot is None:
            return {}
        rows = self.conn.execute(
            "SELECT * FROM events WHERE slot_id = ? ORDER BY seq ASC",
            (slot_id,),
        ).fetchall()
        return {
            "slot": dict(slot),
            "events": [
                {
                    "seq": r["seq"],
                    "event_type": r["event_type"],
                    "registration_id": r["registration_id"],
                    "actor": r["actor"],
                    "payload": json.loads(r["payload"]),
                    "created_at": r["created_at"],
                }
                for r in rows
            ],
        }
