"""成组批次发布的接口级测试：验证路由入参、共享范围与错误分支。"""
from __future__ import annotations

import pytest

from app.routers import release
from app.seed import SEED_ROWS
from app.services.extract_window import ExtractWindowService
from app.services.source_register import SourceRegisterService
from app.store import store


@pytest.fixture(autouse=True)
def clean_store():
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    yield


def _invoke(path: str, payload: dict | None = None):
    """直接调用路由函数，避免依赖 httpx/TestClient。"""
    if path == "/windows":
        return release.create_window(release.CreateWindowPayload(**payload))
    if path == "/submit":
        return release.submit_group(release.ReleaseSubmitPayload(**payload))
    if path.endswith("/retry"):
        manifest_id = int(path.split("/")[2])
        return release.retry_manifest(manifest_id, None)
    if path.endswith("/supplement"):
        manifest_id = int(path.split("/")[2])
        return release.supplement_window(manifest_id, release.SupplementPayload(**payload))
    raise AssertionError(path)


def _source(code: str) -> None:
    service = SourceRegisterService()
    entry = service.register_source({
        "源端编号": code, "系统名称": "路由", "数据域": "订单",
        "记录格式": "JSON", "编码方式": "UTF-8", "日增量级": "1万",
        "抽取方式": "增量抽取", "登记游标": "0",
    })
    service.confirm_register(entry["id"])


def test_release_routes_create_and_reject():
    _source("SOUR-ROUTE")
    windows = [
        _invoke("/windows", {"source_code": "SOUR-ROUTE", "count": 5})["entry"]
        for _ in range(3)
    ]

    result = _invoke("/submit", {"window_ids": [window["id"] for window in windows]})
    assert result["status"] == "已交付"
    assert result["delivered_windows"] == 3

    # 非相邻窗口在路由层被拒绝
    with pytest.raises(Exception) as excinfo:
        _invoke("/submit", {"window_ids": [windows[0]["id"], windows[2]["id"]]})
    assert getattr(excinfo.value, "status_code", None) == 400


def test_release_shared_scope_route():
    _source("SOUR-SCOPE")
    extract = ExtractWindowService()
    windows = []
    for count in (4, 6):
        window = extract.create_window("SOUR-SCOPE", count=count)
        extract.execute_extract(window["id"], record_count=count)
        windows.append(window)

    result = _invoke("/submit", {"window_ids": [window["id"] for window in windows]})
    scope = release.shared_scope(result["manifest_code"])
    expected = [window["窗口编号"] for window in windows]
    assert scope["manifest"]["arrived_windows"] == expected
    assert scope["staging"]["arrived_windows"] == expected
    assert scope["reconcile"]["arrived_windows"] == expected
    assert scope["manifest"]["pending_windows"] == []
