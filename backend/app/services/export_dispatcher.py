"""分发调度器：读取采集仓储批次并生成分发记录。

数据流：result_store -> export_hub

批次状态使用采集仓储的合法状态集合：分发完成后写入 ``已分发``（见
:mod:`app.services.batch_status`），不再写状态机之外的未定义值。重复分发同一个
批次不会再生出第二份状态或分发记录，直接复用已有记录。
"""
from __future__ import annotations

import time
from typing import Any

from app.services import batch_status
from app.services.export_hub import ExportHubService
from app.services.result_store import ResultStoreService
from app.store import store

RESULT_MODULE = "result_store"
DISPATCHABLE_STATUS = "校验通过"


class ExportDispatcher:
    def __init__(self) -> None:
        self._export_svc = ExportHubService()
        self._result_svc = ResultStoreService()

    def _next_log_id(self) -> int:
        rows = store.joint_rows("dispatch_logs")
        return max((int(r.get("id", 0)) for r in rows), default=0) + 1

    def _existing_dispatch(self, batch: dict[str, Any]) -> dict[str, Any] | None:
        return batch_status.dispatch_log_for(batch)

    def dispatch_batch(self, batch_id: int, channel_format: str = "json") -> dict[str, Any]:
        """读取指定采集仓储批次，生成分发记录并写入数据分发通道。

        重复登记同一批次时返回既有分发记录，状态保持唯一且合法。
        """
        batch = store.find(RESULT_MODULE, batch_id)
        if batch is None:
            return {"ok": False, "error": f"结果批次 {batch_id} 不存在"}

        # 读取时先按分发日志校准，旧数据里的未定义状态会被映射为合法状态。
        status = self._result_svc.sync_status(batch)
        existing = self._existing_dispatch(batch)
        if existing is not None:
            return {
                "ok": True,
                "duplicate": True,
                "message": f"批次 {batch_id} 已分发，复用已有分发记录",
                "status": status,
                "log": existing,
                "export_entry": self._export_svc.get_entry(int(existing.get("export_entry_id", 0))),
            }

        if status != DISPATCHABLE_STATUS:
            return {"ok": False, "error": f"批次 {batch_id} 未通过校验，禁止分发"}

        # 创建分发通道记录（跨模块数据产物）
        export_values = {
            "通道编号": f"DISP-{batch_id}-{int(time.time())}",
            "输出格式": channel_format,
            "目标地址": f"s3://exports/{batch_id}.{channel_format}",
            "加密方式": "AES-256",
            "压缩选项": "gzip",
            "分片大小": str(batch.get("有效行数", "0")),
            "重试上限": "3",
            "回调地址": f"https://callback.example.com/{batch_id}",
        }
        export_entry, missing = self._export_svc.create_entry(export_values)
        if export_entry is None:
            return {"ok": False, "error": f"分发记录写入失败：缺少 {missing}"}
        # 通道与批次状态对应：分发完成时通道进入已启用。
        export_entry, _ = self._export_svc.run_action(int(export_entry["id"]), "启用通道")

        # 记录分发日志（联合存储），保留批次状态快照便于追溯。
        log = {
            "id": self._next_log_id(),
            "batch_id": batch_id,
            "export_entry_id": export_entry["id"] if export_entry else None,
            "channel_format": channel_format,
            "target": export_values["目标地址"],
            "dispatched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "status": "已启用",
            "batch_status": batch_status.DISPATCHED_STATUS,
        }
        store.joint_append("dispatch_logs", log)

        # 更新批次状态：只能写入采集仓储允许的状态。
        self._result_svc.set_status(batch, batch_status.DISPATCHED_STATUS)
        self._result_svc.sync_status(batch)

        return {
            "ok": True,
            "status": batch_status.DISPATCHED_STATUS,
            "log": log,
            "export_entry": export_entry,
        }

    def dispatch_all_ready(self) -> dict[str, Any]:
        """对所有校验通过且尚未分发的批次执行自动分发。"""
        batches, _ = self._result_svc.list_entries(
            status=DISPATCHABLE_STATUS, page=1, size=1000000)
        batches = [b for b in batches if self._existing_dispatch(b) is None]
        results: list[dict[str, Any]] = []
        for batch in batches:
            res = self.dispatch_batch(int(batch["id"]))
            results.append(res)
        succeeded = sum(1 for r in results if r.get("ok"))
        return {"ok": True, "dispatched": len(results), "succeeded": succeeded, "results": results}

    def list_logs(self) -> list[dict[str, Any]]:
        return list(store.joint_rows("dispatch_logs"))

    def get_log(self, log_id: int) -> dict[str, Any] | None:
        return store.joint_find("dispatch_logs", log_id)


export_dispatcher = ExportDispatcher()
