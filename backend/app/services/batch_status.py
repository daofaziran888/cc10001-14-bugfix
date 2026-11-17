"""采集仓储批次状态口径：合法状态集合、旧数据识别与迁移判定。

数据流：dispatch_logs -> result_store

历史问题：分发完成后把 ``result_store.status`` 直接写成 ``"已分发"``，但采集仓储
的状态机里并没有这个状态，列表筛选、详情和后续审计都把它当成未定义值。

这里采用“扩展状态机”的方案 B：把 ``已分发`` 纳入采集仓储的合法状态集合，并在读取
批次时结合分发日志做一次幂等的状态校准：

* 合法状态保持不变；
* 旧版本遗留的未定义值 / 旧别名：有分发日志的映射为 ``已分发``，没有日志的按
  历史语义回退到 ``校验通过``；
* 合法状态与分发日志冲突时生成迁移判定结果（``status_migrations``），可追溯。

无论走哪条分支，读到的批次状态都一定属于 :data:`RESULT_STATUSES`。
"""
from __future__ import annotations

import time
from typing import Any

from app.store import store

RESULT_MODULE = "result_store"
DISPATCH_LOG_KEY = "dispatch_logs"
MIGRATION_KEY = "status_migrations"
CREATED_AT_FIELD = "创建时间"

DISPATCHED_STATUS = "已分发"
# 采集仓储允许的状态集合；结果批次的 status 必须落在这里。
RESULT_STATUSES = ["接收中", "已落盘", "校验通过", "校验失败", DISPATCHED_STATUS]
LEGAL_STATUSES = frozenset(RESULT_STATUSES)
# 这两个状态视为流程终点，不再计入待处理。
NON_PENDING_STATUSES = frozenset({"校验失败", DISPATCHED_STATUS})

# 旧数据里可能出现的“未定义”取值；它们都不属于合法状态集合。
UNDEFINED_STATUS_VALUES = frozenset({
    "", "-", "--", "—", "none", "null", "undefined", "nan", "未定义", "未知",
})
# 旧版本把“分发完成”写成过的其它别名（已分发本身现在是合法状态）。
LEGACY_DISPATCHED_ALIASES = frozenset({
    "已分发完成", "分发完成", "已导出", "已投递", "dispatched", "exported",
})


def is_legal_status(value: Any) -> bool:
    """判断任意取值是否属于采集仓储的合法状态集合。"""
    return value is not None and str(value).strip() in LEGAL_STATUSES


def canonical_status(value: Any) -> str | None:
    """把输入状态归一为合法状态；无法识别时返回 None。"""
    text = _text(value)
    if text in LEGAL_STATUSES:
        return text
    if text.casefold() in {item.casefold() for item in LEGACY_DISPATCHED_ALIASES}:
        return DISPATCHED_STATUS
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _safe_int(value: Any, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def latest_dispatch_log(
    batch_id: int,
    logs: list[dict[str, Any]] | None = None,
    since: str | None = None,
) -> dict[str, Any] | None:
    """取该批次最近一条分发日志；日志表是分发完成态的唯一凭证。

    ``since`` 用于避开 id 复用：早于批次创建时间的历史日志不可能描述当前批次。
    """
    rows = store.joint_rows(DISPATCH_LOG_KEY) if logs is None else logs
    matches = [
        row for row in rows
        if _safe_int(row.get("batch_id")) == int(batch_id)
        and (not since or _text(row.get("dispatched_at")) >= _text(since))
    ]
    return matches[-1] if matches else None


def _log_for_batch(
    batch: dict[str, Any], logs: list[dict[str, Any]] | None = None
) -> dict[str, Any] | None:
    return latest_dispatch_log(
        _safe_int(batch.get("id")), logs, since=_text(batch.get(CREATED_AT_FIELD)) or None)


def dispatch_log_for(
    batch: dict[str, Any], logs: list[dict[str, Any]] | None = None
) -> dict[str, Any] | None:
    """取当前批次对应的分发记录（供分发、仓储、详情共用同一口径）。"""
    return _log_for_batch(batch, logs)


def resolve_batch_status(
    batch: dict[str, Any], log: dict[str, Any] | None = None
) -> tuple[str, dict[str, Any] | None]:
    """把批次当前状态与分发日志映射为合法状态。

    返回 ``(合法状态, 迁移判定)``；没有发生冲突时判定为 ``None``。
    """
    raw = batch.get("status")
    text = _text(raw)
    log_id = log.get("id") if log else None
    verdict: dict[str, Any] | None = None

    if text in LEGAL_STATUSES:
        status = text
    elif text.casefold() in {item.casefold() for item in LEGACY_DISPATCHED_ALIASES}:
        status = DISPATCHED_STATUS if log else "校验通过"
        verdict = {
            "from_status": raw,
            "to_status": status,
            "decision": "migrated",
            "reason": "旧分发状态别名，依据分发日志映射为合法批次状态",
            "log_id": log_id,
        }
    else:
        # 未定义值（空值、None、undefined 等）和未知取值都按历史脏数据修复。
        status = DISPATCHED_STATUS if log else "校验通过"
        undefined = text.casefold() in {item.casefold() for item in UNDEFINED_STATUS_VALUES}
        verdict = {
            "from_status": raw,
            "to_status": status,
            "decision": "migrated",
            "reason": (
                ("未定义状态" if undefined else "未知状态")
                + ("且存在分发记录，迁移为已分发" if log
                   else "且无分发记录，按历史语义回退为校验通过")
            ),
            "log_id": log_id,
        }

    # 状态与分发记录必须对应：日志是分发完成的事实依据，冲突时以日志为准。
    if status == DISPATCHED_STATUS and log is None:
        status = "校验通过"
        verdict = {
            "from_status": raw,
            "to_status": status,
            "decision": "migrated",
            "reason": "状态为已分发但缺少分发记录，回退为校验通过以保证状态可验证",
            "log_id": None,
        }
    elif status != DISPATCHED_STATUS and log is not None:
        conflict = status in {"接收中", "已落盘", "校验失败"}
        status = DISPATCHED_STATUS
        verdict = {
            "from_status": raw,
            "to_status": status,
            "decision": "kept" if conflict else "migrated",
            "reason": (
                f"分发记录与「{_text(raw)}」冲突，以分发记录为准映射为已分发"
                if conflict
                else "分发记录已存在，按分发记录将批次状态回写为已分发"
            ),
            "log_id": log_id,
            "conflict": conflict,
        }

    return status, verdict


def _record_migration(batch: dict[str, Any], verdict: dict[str, Any]) -> None:
    """写入迁移判定结果，按批次与状态变化去重，重复读取不会多出一份判定。"""
    batch_id = _safe_int(batch.get("id"))
    from_status = _text(verdict.get("from_status"))
    to_status = _text(verdict.get("to_status"))
    reason = _text(verdict.get("reason"))
    for row in store.joint_rows(MIGRATION_KEY):
        if (
            _safe_int(row.get("batch_id")) == batch_id
            and _text(row.get("from_status")) == from_status
            and _text(row.get("to_status")) == to_status
            and _text(row.get("reason")) == reason
        ):
            return
    rows = store.joint_rows(MIGRATION_KEY)
    record = {
        "id": max((_safe_int(r.get("id"), 0) for r in rows), default=0) + 1,
        "batch_id": batch_id,
        "批次编号": batch.get("批次编号"),
        "from_status": from_status,
        "to_status": to_status,
        "decision": verdict.get("decision", "migrated"),
        "reason": reason,
        "conflict": bool(verdict.get("conflict")),
        "log_id": verdict.get("log_id"),
        "migrated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    store.joint_append(MIGRATION_KEY, record)


def apply_batch_status(
    batch: dict[str, Any],
    *,
    logs: list[dict[str, Any]] | None = None,
    persist: bool = True,
) -> str:
    """校准单个批次的状态并回写；重复调用幂等。"""
    log = _log_for_batch(batch, logs)
    status, verdict = resolve_batch_status(batch, log)
    # 用 != 而不是文本比较：顺带把 " 已分发 " 这类带空白的旧值归一。
    if batch.get("status") != status:
        batch["status"] = status
        batch["pending"] = status not in NON_PENDING_STATUSES
    if status == DISPATCHED_STATUS:
        batch["pending"] = False
    if persist and verdict:
        _record_migration(batch, verdict)
    return status


def write_batch_status(
    batch: dict[str, Any], target: str, *, persist: bool = True
) -> tuple[str, dict[str, Any] | None]:
    """业务动作写入目标状态：状态机之外的值一律拒绝，并与分发记录保持对应。"""
    if not is_legal_status(target):
        return _text(batch.get("status")), None
    log = _log_for_batch(batch)
    status = target
    verdict: dict[str, Any] | None = None
    if not is_legal_status(batch.get("status")):
        # 目标合法，但当前还是历史未定义值：先把这次修复记成迁移判定。
        verdict = {
            "from_status": batch.get("status"),
            "to_status": target,
            "decision": "migrated",
            "reason": "写状态前修复历史未定义值，目标状态属于合法状态集合",
            "log_id": log.get("id") if log else None,
        }
    if log is not None and target != DISPATCHED_STATUS:
        # 分发记录是完成态的事实依据，后续动作不能把已分发批次改回未分发状态。
        status = DISPATCHED_STATUS
        verdict = {
            "from_status": target,
            "to_status": DISPATCHED_STATUS,
            "decision": "kept",
            "reason": "批次已有分发记录，拒绝回退为非分发状态",
            "log_id": log.get("id"),
            "conflict": True,
        }
    elif log is None and target == DISPATCHED_STATUS:
        status = "校验通过"
        verdict = {
            "from_status": target,
            "to_status": status,
            "decision": "migrated",
            "reason": "目标状态为已分发但缺少分发记录，回退为校验通过",
            "log_id": None,
        }
    batch["status"] = status
    batch["pending"] = status not in NON_PENDING_STATUSES
    if persist and verdict:
        _record_migration(batch, verdict)
    return status, verdict


def migrate_legacy_batch_statuses() -> list[dict[str, Any]]:
    """扫描全部采集仓储批次，修复未定义状态并生成可追溯的迁移判定结果。"""
    logs = store.joint_rows(DISPATCH_LOG_KEY)
    for batch in list(store.rows(RESULT_MODULE)):
        apply_batch_status(batch, logs=logs)
    return list(store.joint_rows(MIGRATION_KEY))


def list_migrations() -> list[dict[str, Any]]:
    return list(store.joint_rows(MIGRATION_KEY))
