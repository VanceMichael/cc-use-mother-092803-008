"""SQLite 事件存储。

只做三件事：
1. 事件只追加（append-only），每条事件在赛事流内带严格递增版本号 seq，
   任何状态变更都必须先读到最新版本再追加，靠 ``seq`` 唯一约束防并发覆盖；
2. ``requests`` 表以 request_id 记录每条命令的首个响应，保证命令幂等；
3. ``notices`` 表以 (channel, notice_id) 对支付/退款通知去重，重放不再生效。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     TEXT NOT NULL UNIQUE,
    stream_id    TEXT NOT NULL,
    stream_seq   INTEGER NOT NULL,
    event_type   TEXT NOT NULL,
    data         TEXT NOT NULL,
    actor        TEXT NOT NULL,
    occurred_at  TEXT NOT NULL,
    UNIQUE(stream_id, stream_seq)
);
CREATE INDEX IF NOT EXISTS idx_events_stream ON events(stream_id, seq);

CREATE TABLE IF NOT EXISTS requests (
    request_id  TEXT PRIMARY KEY,
    accepted    INTEGER NOT NULL,
    state       TEXT NOT NULL,
    message     TEXT NOT NULL,
    data        TEXT NOT NULL,
    created_at  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS notices (
    channel     TEXT NOT NULL,
    notice_id   TEXT NOT NULL,
    first_seen  TEXT NOT NULL,
    PRIMARY KEY(channel, notice_id)
);
"""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class EventStore:
    """线程安全的 SQLite 事件存储；每次提交是一个独立事务。"""

    def __init__(self, path: str = ":memory:") -> None:
        # check_same_thread=False 后，由 MarathonEntryService 的锁串行化所有写入，
        # 只读快照在独立连接上进行，互不阻塞。
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.executescript(SCHEMA)

    def close(self) -> None:
        self._conn.close()

    # ---------- 事件 ----------

    def load_events(self, stream_id: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT event_id, stream_seq, event_type, data, actor, occurred_at "
            "FROM events WHERE stream_id=? ORDER BY seq",
            (stream_id,),
        ).fetchall()
        return [self._row_to_event(r) for r in rows]

    def load_all_events(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT event_id, stream_id, stream_seq, event_type, data, actor, occurred_at "
            "FROM events ORDER BY seq"
        ).fetchall()
        return [self._row_to_event(r, with_stream=True) for r in rows]

    def commit(
        self,
        *,
        stream_id: str,
        expected_stream_seq: int,
        event_specs: list[tuple[str, dict[str, Any], str, str]],
        notices: list[tuple[str, str]],
        request_id: str,
        result: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """事件追加、通知去重标记、命令响应在同一个事务内提交，同生共死。"""
        if not event_specs:
            raise ValueError("没有可提交的事件")
        persisted: list[dict[str, Any]] = []
        conn = self._conn
        conn.execute("BEGIN IMMEDIATE")
        try:
            current = conn.execute(
                "SELECT COALESCE(MAX(stream_seq),0) FROM events WHERE stream_id=?",
                (stream_id,),
            ).fetchone()[0]
            if current != expected_stream_seq:
                raise ConcurrentModification(stream_id, expected_stream_seq, current)
            next_seq = current
            for event_type, data, event_id, occurred_at in event_specs:
                next_seq += 1
                conn.execute(
                    "INSERT INTO events(event_id, stream_id, stream_seq, event_type, data, actor, occurred_at) "
                    "VALUES(?,?,?,?,?,?,?)",
                    (
                        event_id,
                        stream_id,
                        next_seq,
                        event_type,
                        json.dumps(data, ensure_ascii=False, sort_keys=True),
                        data.get("by", "system"),
                        occurred_at,
                    ),
                )
                persisted.append(
                    {
                        "event_id": event_id,
                        "stream_id": stream_id,
                        "stream_seq": next_seq,
                        "event_type": event_type,
                        "data": data,
                        "actor": data.get("by", "system"),
                        "occurred_at": occurred_at,
                    }
                )
            for channel, notice_id in notices:
                conn.execute(
                    "INSERT OR IGNORE INTO notices(channel, notice_id, first_seen) VALUES(?,?,?)",
                    (channel, notice_id, utc_now()),
                )
            conn.execute(
                "INSERT OR IGNORE INTO requests(request_id, accepted, state, message, data, created_at) "
                "VALUES(?,?,?,?,?,?)",
                (
                    request_id,
                    1 if result["accepted"] else 0,
                    result["state"],
                    result["message"],
                    json.dumps(result.get("data", {}), ensure_ascii=False, sort_keys=True),
                    utc_now(),
                ),
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        return persisted

    # ---------- 命令幂等 ----------

    def get_response(self, request_id: str) -> Optional[dict[str, Any]]:
        row = self._conn.execute(
            "SELECT accepted, state, message, data FROM requests WHERE request_id=?",
            (request_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "accepted": bool(row["accepted"]),
            "state": row["state"],
            "message": row["message"],
            "data": json.loads(row["data"]),
        }

    def save_response(self, request_id: str, result: dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO requests(request_id, accepted, state, message, data, created_at) "
            "VALUES(?,?,?,?,?,?)",
            (
                request_id,
                1 if result["accepted"] else 0,
                result["state"],
                result["message"],
                json.dumps(result.get("data", {}), ensure_ascii=False, sort_keys=True),
                utc_now(),
            ),
        )

    # ---------- 通知去重 ----------

    def notice_seen(self, channel: str, notice_id: str) -> bool:
        row = self._conn.execute(
            "SELECT 1 FROM notices WHERE channel=? AND notice_id=?",
            (channel, notice_id),
        ).fetchone()
        return row is not None

    def mark_notice(self, channel: str, notice_id: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO notices(channel, notice_id, first_seen) VALUES(?,?,?)",
            (channel, notice_id, utc_now()),
        )

    # ---------- 内部 ----------

    @staticmethod
    def _row_to_event(row: sqlite3.Row, with_stream: bool = False) -> dict[str, Any]:
        event = {
            "event_id": row["event_id"],
            "stream_seq": row["stream_seq"],
            "event_type": row["event_type"],
            "data": json.loads(row["data"]),
            "actor": row["actor"],
            "occurred_at": row["occurred_at"],
        }
        if with_stream:
            event["stream_id"] = row["stream_id"]
        return event


class ConcurrentModification(RuntimeError):
    def __init__(self, stream_id: str, expected: int, actual: int) -> None:
        super().__init__(
            f"赛事 {stream_id} 事件版本冲突：期望 {expected}，实际 {actual}"
        )
