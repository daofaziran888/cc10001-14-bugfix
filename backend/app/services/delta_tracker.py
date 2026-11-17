"""增量追踪器：同步水位驱动采集仓储的增量数据持久化。

数据流：delta_sync -> result_store
"""
from __future__ import annotations

import time
from typing import Any

from app.store import store

SYNC_MODULE = "delta_sync"
RESULT_MODULE = "result_store"


class DeltaTracker:
    def __init__(self) -> None:
        pass

    def _next_sync_id(self) -> int:
        rows = store.joint_rows("sync_positions")
        return max((int(r.get("id", 0)) for r in rows), default=0) + 1

    def capture_increment(self, sync_id: int) -> dict[str, Any]:
        """基于同步水位捕获增量，并驱动仓储持久化新批次。"""
        sync = store.find(SYNC_MODULE, sync_id)
        if sync is None:
            return {"ok": False, "error": f"同步水位 {sync_id} 不存在"}

        last_watermark = str(sync.get("上次水位", "0") or "0")
        current_watermark = str(sync.get("当前水位", "0") or "0")

        # 模拟增量捕获：计算新增行数
        try:
            captured = max(0, (int(sync["id"]) * 10) - (int(sync["id"]) * 5))
        except ValueError:
            captured = 0

        # 将增量数据持久化到采集仓储（跨模块数据产物）
        if captured > 0:
            batch = {
                "id": max((int(r.get("id", 0)) for r in store.rows(RESULT_MODULE)), default=0) + 1,
                "批次编号": f"DELTA-{sync_id}-{int(time.time())}",
                "关联任务": str(sync.get("源数据集", "")),
                "原始行数": str(captured),
                "有效行数": str(captured),
                "存储路径": f"/store/delta/{sync_id}/{current_watermark}.json",
                "压缩格式": "snappy",
                "校验摘要": f"wm-{current_watermark}",
                "落地时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                "创建时间": time.strftime("%Y-%m-%d %H:%M:%S"),
                "status": "接收中",
                "pending": True,
                "abnormal": False,
            }
            store.rows(RESULT_MODULE).append(batch)

        # 更新水位并记录同步位置（联合存储）
        sync["上次水位"] = current_watermark
        sync["捕获行数"] = str(captured)
        sync["最后同步"] = time.strftime("%Y-%m-%d %H:%M:%S")
        sync["status"] = "运行中"

        position = {
            "id": self._next_sync_id(),
            "sync_id": sync_id,
            "last_watermark": last_watermark,
            "current_watermark": current_watermark,
            "captured_rows": captured,
            "synced_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        store.joint_append("sync_positions", position)

        return {
            "ok": True,
            "captured_rows": captured,
            "position": position,
            "batch_id": batch["id"] if captured > 0 else None,
        }

    def sync_all(self) -> dict[str, Any]:
        """对所有待初始化/运行中的同步任务执行增量捕获。"""
        tasks = [s for s in store.rows(SYNC_MODULE) if s.get("status") in ("待初始化", "运行中", "追赶延迟")]
        results: list[dict[str, Any]] = []
        for task in tasks:
            res = self.capture_increment(int(task["id"]))
            results.append(res)
        total_captured = sum(r.get("captured_rows", 0) for r in results)
        return {"ok": True, "synced": len(results), "total_captured": total_captured, "results": results}

    def list_positions(self) -> list[dict[str, Any]]:
        return list(store.joint_rows("sync_positions"))

    def get_position(self, position_id: int) -> dict[str, Any] | None:
        return store.joint_find("sync_positions", position_id)


delta_tracker = DeltaTracker()
