"""赛事报名管理 的输入输出约定。"""
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Request:
    """一次服务调用。

    actor:       调用方标识（工作人员账号 / 报名系统 / 支付网关）。
    action:      见 app.actions 中的动作常量。
    payload:     动作参数。
    request_id:  调用方幂等键（如支付通知流水号）；同键重放只返回首次结果。
    created_at:  调用方时间（可选），仅用于展示。
    """

    actor: str
    action: str
    payload: dict[str, Any]
    request_id: str
    created_at: datetime = field(default_factory=_utcnow)


@dataclass
class Result:
    accepted: bool
    state: str
    message: str
    data: dict[str, Any] = field(default_factory=dict)

    def ok(message: str, state: str = "ok", **data: Any) -> "Result":
        return Result(True, state, message, data)

    def reject(message: str, state: str = "rejected", **data: Any) -> "Result":
        return Result(False, state, message, data)


def validate_request(request: Request) -> None:
    if not request.actor or not request.action or not request.request_id:
        raise ValueError("请求缺少身份、动作或幂等键")
    if not isinstance(request.payload, dict):
        raise TypeError("请求数据必须是对象")


class DomainError(Exception):
    """可预期的业务规则冲突（映射为 accepted=False，而非 500）。"""

    def __init__(self, message: str, code: str = "rule_violation") -> None:
        super().__init__(message)
        self.message = message
        self.code = code


class NotFound(DomainError):
    def __init__(self, message: str) -> None:
        super().__init__(message, "not_found")


class Forbidden(DomainError):
    def __init__(self, message: str) -> None:
        super().__init__(message, "forbidden")


class Conflict(DomainError):
    def __init__(self, message: str) -> None:
        super().__init__(message, "conflict")
