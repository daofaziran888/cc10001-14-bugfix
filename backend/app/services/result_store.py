"""采集仓储业务规则：状态流转、字段校验与筛选口径都收在这里。

批次状态一律使用 :mod:`app.services.batch_status` 里的合法状态集合；读取时会把
历史遗留的未定义状态按分发日志校准并回写，保证列表、详情和后续审计看到同一状态。
"""
from __future__ import annotations

import time
from typing import Any

from app.store import store
from app.services import batch_status

MODULE = "result_store"
LIST_FIELDS = ["批次编号", "关联任务", "原始行数", "有效行数", "存储路径", "压缩格式", "校验摘要", "落地时间"]
REQUIRED_FIELDS = ["批次编号", "关联任务", "原始行数"]
STATUS_ORDER = list(batch_status.RESULT_STATUSES)
STATUSES = STATUS_ORDER
# 由采集仓储 + 分发日志派生出来的只读展示字段，不接受登记时提交。
DERIVED_FIELDS = ["分发状态", "分发记录", "分发通道"]
ACTION_RULES = {"触发校验": "校验通过", "标记通过": "校验失败", "标记失败": "已落盘"}
NEGATIVE_ACTIONS = []


class ResultStoreService:
    def __init__(self) -> None:
        # 启动后第一次进入采集仓储时先修复历史遗留状态，保证后续读取口径一致。
        batch_status.migrate_legacy_batch_statuses()

    @staticmethod
    def sync_status(entry: dict[str, Any], logs: list[dict[str, Any]] | None = None) -> str:
        """校准批次状态并补齐分发相关字段；返回校准后的合法状态。"""
        log_rows = store.joint_rows(batch_status.DISPATCH_LOG_KEY) if logs is None else logs
        status = batch_status.apply_batch_status(entry, logs=log_rows)
        log = batch_status.dispatch_log_for(entry, log_rows)
        entry["分发状态"] = batch_status.DISPATCHED_STATUS if log else "未分发"
        entry["分发记录"] = int(log["id"]) if log and log.get("id") is not None else None
        entry["分发通道"] = log.get("export_entry_id") if log else None
        return status

    def _refresh(self) -> list[dict[str, Any]]:
        rows = store.rows(MODULE)
        logs = store.joint_rows(batch_status.DISPATCH_LOG_KEY)
        for row in rows:
            self.sync_status(row, logs)
        return rows

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = self._refresh()
        # 状态筛选只接受合法批次状态；非法值返回空集合而不是把未定义值混进结果。
        if status:
            wanted = batch_status.canonical_status(status)
            if wanted is None:
                return [], 0
            rows = [row for row in rows if row.get("status") == wanted]
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("批次编号", "")).casefold()]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None
        logs = store.joint_rows(batch_status.DISPATCH_LOG_KEY)
        self.sync_status(entry, logs)
        detail = dict(entry)
        detail["dispatch_logs"] = [
            dict(log) for log in logs
            if str(log.get("batch_id")) == str(entry_id)
        ]
        detail["status_migrations"] = [
            dict(item) for item in store.joint_rows(batch_status.MIGRATION_KEY)
            if str(item.get("batch_id")) == str(entry_id)
        ]
        return detail

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        # 登记只接受合法初始状态，历史未定义值不能从入口再次写入。
        entry["status"] = STATUS_ORDER[0]
        entry[batch_status.CREATED_AT_FIELD] = time.strftime("%Y-%m-%d %H:%M:%S")
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        self.sync_status(entry)
        return entry, []

    def set_status(self, entry: dict[str, Any], target: str) -> bool:
        """把批次状态写成合法状态；非法目标一律拒绝。"""
        if not batch_status.is_legal_status(target):
            return False
        status, _ = batch_status.write_batch_status(entry, target)
        return status == target and entry.get("status") == target

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"结果批次 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于采集仓储可执行范围"
        # 已分发批次的状态以分发记录为准，不能被普通动作改回未分发。
        if self.sync_status(entry) == batch_status.DISPATCHED_STATUS:
            return None, f"结果批次 {entry_id} 已分发，批次状态以分发记录为准"
        target = ACTION_RULES[action]
        if not self.set_status(entry, target):
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        self.sync_status(entry)
        return entry, f"结果批次已{action}"

    def list_migrations(self) -> list[dict[str, Any]]:
        return batch_status.list_migrations()
