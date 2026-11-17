"""采集仓储接口：维护结果批次，覆盖触发校验、标记通过、标记失败等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services import batch_status
from app.services.result_store import ResultStoreService

router = APIRouter(prefix="/api/result_store", tags=["采集仓储"])

service = ResultStoreService()

LIST_FIELDS = ["批次编号", "关联任务", "原始行数", "有效行数", "存储路径", "压缩格式", "校验摘要", "落地时间"]
STATUSES = list(batch_status.RESULT_STATUSES)


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段（含派生的分发字段），忽略分页等保留参数。"""
    accepted = set(LIST_FIELDS) | {"分发状态", "分发记录", "分发通道"}
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in accepted and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按批次编号检索"),
    status: str | None = Query(default=None, description="接收中、已落盘、校验通过、校验失败、已分发"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、批次编号关键字与状态过滤采集仓储列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status", "分发状态", "分发记录"])
    for item in items:
        row = []
        for value in [
            item.get("id"), *(item.get(field, "") for field in LIST_FIELDS),
            item.get("status", ""), item.get("分发状态", ""), item.get("分发记录", ""),
        ]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="result_store.csv"'})


@router.get("/status-options")
def list_status_options() -> dict:
    """返回采集仓储允许的状态集合，页面筛选与详情共用同一份口径。"""
    return {"statuses": STATUSES, "dispatched": batch_status.DISPATCHED_STATUS}


@router.get("/status-migrations")
def list_status_migrations() -> dict:
    """返回历史未定义批次状态的迁移判定结果，用于审计追溯。"""
    items = service.list_migrations()
    return {"items": items, "total": len(items), "statuses": STATUSES}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条结果批次明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"结果批次 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条结果批次，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="结果批次已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条结果批次执行触发校验、标记通过、标记失败；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
