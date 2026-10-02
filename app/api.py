"""赛事报名管理 的轻量本地调用入口。

从标准输入读取一条 JSON 命令，输出一条 JSON 结果。数据库文件由环境变量
``MARATHON_DB`` 指定，默认 ``marathon.db``（进程间共享事件流）；
设为 ``:memory:`` 时仅用于单次进程内测试。

示例::

    echo '{"actor":"admin-1","action":"create_race",
           "payload":{"race_id":"bj2026","name":"北京马拉松"},
           "request_id":"r-1"}' | python3 -m app.api
"""
import json
import os
import sys
from datetime import datetime, timezone

from .contracts import Request
from .service import MarathonEntryService
from .store import EventStore


def main() -> int:
    raw = sys.stdin.read().strip()
    if not raw:
        return 2
    item = json.loads(raw)
    request = Request(
        actor=str(item.get("actor", "")),
        action=str(item.get("action", "")),
        payload=dict(item.get("payload", {})),
        request_id=str(item.get("request_id", "")),
        created_at=datetime.now(timezone.utc),
    )
    db_path = os.environ.get("MARATHON_DB", "marathon.db")
    service = MarathonEntryService(EventStore(db_path))
    try:
        result = service.handle(request)
    finally:
        service.close()
    print(json.dumps(result.as_dict(), ensure_ascii=False))
    return 0 if result.accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
