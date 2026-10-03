"""赛事报名管理 的轻量本地调用入口。

用法：
    MARATHON_DB=marathon.db python -m app.api < request.json

request.json 可以是单个请求对象：
    {"actor": "org", "action": "event.create",
     "request_id": "req-1", "payload": {"name": "北京马拉松"}}

也可以是一批请求（按顺序在各自独立事务中执行）：
    {"requests": [ {...}, {...} ]}

退出码：全部受理 0；存在业务拒绝 1；调用方格式错误 2。
"""
from __future__ import annotations

import json
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

from .contracts import Request
from .service import MarathonEntryService


def _build_request(item: dict[str, Any]) -> Request:
    created_raw = item.get("created_at")
    created_at = datetime.fromisoformat(created_raw) if created_raw else datetime.now(timezone.utc)
    return Request(
        actor=str(item.get("actor", "")),
        action=str(item.get("action", "")),
        payload=dict(item.get("payload", {})),
        request_id=str(item.get("request_id") or f"cli-{uuid.uuid4().hex}"),
        created_at=created_at,
    )


def _encode(result: Any) -> dict[str, Any]:
    return {
        "accepted": result.accepted,
        "state": result.state,
        "message": result.message,
        "data": result.data,
    }


def main(argv: list[str] | None = None) -> int:
    raw = sys.stdin.read().strip()
    if not raw:
        sys.stderr.write("stdin 需要一个 JSON 请求或 {\"requests\": [...]}\n")
        return 2
    try:
        item = json.loads(raw)
    except json.JSONDecodeError as exc:
        sys.stderr.write(f"JSON 解析失败: {exc}\n")
        return 2

    if isinstance(item, dict) and "requests" in item:
        requests = [_build_request(x) for x in item["requests"]]
    else:
        requests = [_build_request(item)]

    db_path = os.environ.get("MARATHON_DB", ":memory:")
    service = MarathonEntryService(db_path)
    outputs = []
    all_accepted = True
    try:
        for request in requests:
            result = service.handle(request)
            outputs.append(_encode(result))
            all_accepted = all_accepted and result.accepted
    except (ValueError, TypeError) as exc:
        sys.stderr.write(f"请求非法: {exc}\n")
        return 2
    finally:
        service.close()

    payload: Any = outputs[0] if len(outputs) == 1 else {"results": outputs}
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0 if all_accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
