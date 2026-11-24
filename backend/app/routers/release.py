"""成组批次发布接口：一次提交整组相邻窗口，并提供重试、补数与共享范围查询。

资源路由（/api/{module}）负责通用查询与动作；本路由补充成组发布特有的
批量入参和多模块范围视图，避免把窗口列表塞进通用动作接口。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.arrival_reconcile import ArrivalReconcileService
from app.services.release_manifest import ReleaseManifestService
from app.services.staging_area import StagingAreaService
from app.services.transfer_batch import TransferBatchService

router = APIRouter(prefix="/api/release", tags=["成组批次发布"])


class ReleaseSubmitPayload(BaseModel):
    """成组提交入参：窗口 id 或窗口编号二选一，二者都给时按 id 在前合并去重。"""

    window_ids: list[int] = Field(default_factory=list)
    window_codes: list[str] = Field(default_factory=list)
    target_code: str | None = None
    auto_extract: bool = False
    fail_window: int | str | None = None
    fail_after_rows: int | None = None


class ReleaseRetryPayload(BaseModel):
    """重试保留空载荷：按原申请重提，不再注入演练故障。"""


class SupplementPayload(BaseModel):
    window_id: int | str
    record_count: int | None = None


class CreateWindowPayload(BaseModel):
    source_code: str
    count: int = 10
    window_start: str | None = None
    window_end: str | None = None
    extract: bool = True


class ExtractWindowPayload(BaseModel):
    record_count: int | None = None


def _manifest_service() -> ReleaseManifestService:
    return ReleaseManifestService()


@router.post("/windows")
def create_window(payload: CreateWindowPayload) -> dict[str, Any]:
    """为同一源端连续创建相邻窗口，可选立即抽取，供成组提交前准备数据。"""
    from app.services.extract_window import ExtractWindowService

    service = ExtractWindowService()
    try:
        window = service.create_window(
            payload.source_code,
            count=payload.count,
            window_start=payload.window_start,
            window_end=payload.window_end,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    result: dict[str, Any] = {"ok": True, "entry": window}
    if payload.extract:
        extracted = service.execute_extract(window["id"], record_count=payload.count)
        if extracted.get("error"):
            raise HTTPException(status_code=400, detail=extracted["error"])
        result["extract"] = extracted
    return result


@router.post("/windows/{window_id}/extract")
def extract_window(window_id: int, payload: ExtractWindowPayload | None = None) -> dict[str, Any]:
    """执行单个窗口抽取；抽取写满后窗口闭合，才可进入成组交付。"""
    from app.services.extract_window import ExtractWindowService

    service = ExtractWindowService()
    window = service.get_entry(window_id)
    if window is None:
        raise HTTPException(status_code=404, detail=f"抽取窗口 {window_id} 不存在")
    count = (payload.record_count if payload else None) or int(window.get("抽取条数") or 10)
    result = service.execute_extract(window_id, record_count=count)
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return {"ok": True, **result}


@router.post("/windows/{window_id}/supplement")
def supplement_extract(window_id: int, payload: ExtractWindowPayload | None = None) -> dict[str, Any]:
    """单独补齐单个窗口的抽取产物；不影响同组其他窗口。"""
    from app.services.extract_window import ExtractWindowService

    result = ExtractWindowService().supplement_extract(
        window_id, record_count=payload.record_count if payload else None
    )
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return {"ok": True, **result}


@router.post("/manifests")
def create_manifest(payload: ReleaseSubmitPayload) -> dict[str, Any]:
    """登记成组发布清单但不执行交付，供值班人员先核对窗口范围再提交。"""
    service = _manifest_service()
    row, missing = service.create_entry({
        "window_ids": payload.window_ids,
        "window_codes": payload.window_codes,
        "target_code": payload.target_code,
    })
    if row is None:
        raise HTTPException(status_code=400, detail="；".join(missing))
    return {"ok": True, "message": "成组发布清单已创建", "entry": service.get_entry(row["id"])}


@router.post("/submit")
def submit_group(payload: ReleaseSubmitPayload) -> dict[str, Any]:
    """一次提交整组相邻窗口：创建清单并立即执行成组批次发布。"""
    service = _manifest_service()
    result = service.submit_group(
        window_ids=payload.window_ids,
        window_codes=payload.window_codes,
        target_code=payload.target_code,
        auto_extract=payload.auto_extract,
        fail_window=payload.fail_window,
        fail_after_rows=payload.fail_after_rows,
    )
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["message"])
    return result


class ReleaseRetryPayload(BaseModel):
    """重试保留空载荷，便于值班人员直接按原申请重提，不再注入演练故障。"""


@router.post("/manifests/{manifest_id}/retry")
def retry_manifest(manifest_id: int, payload: ReleaseRetryPayload | None = None) -> dict[str, Any]:
    """按原申请重试整组任务：已交付成员跳过，失败成员从原窗口断点续写。"""
    service = _manifest_service()
    result = service.retry(manifest_id)
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.post("/manifests/{manifest_id}/supplement")
def supplement_window(manifest_id: int, payload: SupplementPayload) -> dict[str, Any]:
    """单独补上某个故障窗口的数据，补完后仍需重新提交原申请。"""
    service = _manifest_service()
    result = service.supplement_window(manifest_id, payload.window_id, payload.record_count)
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.get("/manifests/{manifest_id}")
def get_manifest(manifest_id: int) -> dict[str, Any]:
    """读取清单详情：含每个成员的实际行数、到达状态与交付次数。"""
    service = _manifest_service()
    entry = service.get_entry(manifest_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"成组发布清单 {manifest_id} 不存在")
    return entry


@router.get("/scope/{manifest_code}")
def shared_scope(manifest_code: str) -> dict[str, Any]:
    """传输、暂存、对账共享的成组范围视图，区分已抵达窗口与待补齐窗口。"""
    manifest = _manifest_service().get_manifest(manifest_code)
    if manifest is None:
        raise HTTPException(status_code=404, detail=f"发布清单 {manifest_code} 不存在")
    return {
        "manifest": _manifest_service().manifest_scope(manifest_code),
        "transfer": TransferBatchService().transfer_scope(manifest_code),
        "staging": StagingAreaService().staging_scope(manifest_code),
        "reconcile": ArrivalReconcileService().reconcile_manifest(manifest_code),
    }
