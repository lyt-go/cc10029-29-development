"""成组批次发布领域服务：把相邻抽取窗口组织为一次成组提交，生成共享发布清单。

本模块是「多窗口批量操作」的领域入口：值班人员一次选择整组相邻窗口后，服务
校验窗口是否相邻、抽取是否完成，再按窗口逐个写入传输产物与暂存产物，并把
每个窗口的交付状态、实际行数、到达状态和交付次数登记到同一份发布清单。

发布清单（release manifest）是传输与到达对账共同消费的唯一事实来源：

- 传输侧按清单里的成员窗口逐窗写入，已交付成员在重试中直接跳过；
- 暂存侧按清单核对每个窗口的已交付范围；
- 对账侧据清单区分「已抵达窗口」与「尚待补齐窗口」。

清单同时写入 store（供列表/详情接口读取）和 ``data/releases/<发布编号>.json``
（供跨模块、跨进程读取）。每次成员写满后都会刷新清单，因此中途失败时清单里
保留的是「部分完成」的真实水位，不会把未写完的窗口误判为已交付。
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _as_int(value: Any, default: int = 0) -> int:
    """把字符串/数字安全转成整数，解析失败时返回默认值。"""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _next_id(rows: list[dict[str, Any]]) -> int:
    return max((_as_int(row.get("id")) for row in rows), default=0) + 1


class ReleaseManifestService(BaseModuleService):
    """相邻窗口成组发布：发布清单创建、逐窗交付、失败恢复与共享范围查询。"""

    MODULE = "release_manifest"
    ENTITY = "成组发布清单"
    OPERATION_TERM = "发布"
    LIST_FIELDS = [
        "发布编号", "窗口范围", "窗口数量", "目标编号", "有效窗口数",
        "已交付窗口数", "待补齐窗口数", "交付状态", "发布水位",
    ]
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER = ["草稿", "待发布", "发布中", "部分交付", "已交付", "已撤回"]
    ACTION_RULES = {
        "发布成组批次": "发布中",
        "重试发布": "发布中",
        "确认交付": "已交付",
        "撤回发布": "已撤回",
    }
    NEGATIVE_ACTIONS = ["撤回发布"]

    # ------------------------------------------------------------------
    # 创建与登记
    # ------------------------------------------------------------------
    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """按窗口 id / 窗口编号创建待发布的成组清单，不立即执行交付。"""
        try:
            windows = self._windows_from_values(values)
            self._validate_adjacent(windows)
        except ValueError as exc:
            return None, [str(exc)]
        target_code = self._target_from_values(values)
        manifest = self._build_manifest(windows, target_code)
        row = self._register_manifest(manifest)
        return row, []

    def submit_group(
        self,
        window_ids: list[int] | None = None,
        window_codes: list[str] | None = None,
        target_code: str | None = None,
        *,
        fail_window: Any = None,
        fail_after_rows: int | None = None,
        auto_extract: bool = False,
    ) -> dict[str, Any]:
        """一次提交整组相邻窗口：创建发布清单并立即执行成组发布。

        ``fail_window`` / ``fail_after_rows`` 是值班演练开关：模拟该窗口的写入
        请求只写完前 N 行就中断。该窗口因此没有完成抽取，按规则不能对外交付，
        其余窗口照常交付，清单进入「部分交付」。
        """
        windows = self._windows_from_values({
            "window_ids": window_ids or [],
            "window_codes": window_codes or [],
        })
        try:
            self._validate_adjacent(windows)
        except ValueError as exc:
            return {"ok": False, "error": str(exc), "message": str(exc)}

        if auto_extract:
            for window in windows:
                if not self._window_artifact_rows(window):
                    self._extract_window(window)

        faults: dict[int, int] = {}
        if fail_window not in (None, "", []):
            target = self._pick_window(windows, fail_window)
            if target is not None:
                position = self._inject_extraction_fault(target, fail_after_rows)
                faults[_as_int(target.get("id"))] = position

        manifest = self._build_manifest(windows, target_code or "TARG-0001")
        for member in manifest["members"]:
            position = faults.get(_as_int(member.get("window_id")))
            if position is not None:
                member["failure_position"] = position
                member["last_failure_position"] = position
                member["fault_injected"] = True
        row = self._register_manifest(manifest)
        result = self.publish(row["id"])
        result["ok"] = result.get("status") in ("已交付", "部分交付")
        return result

    # 兼容不同调用方命名
    submit_windows = submit_group
    batch_submit = submit_group
    publish_group = submit_group

    def _windows_from_values(self, values: dict[str, Any]) -> list[dict[str, Any]]:
        """从多种入参命名里解析窗口列表，并保持调用方选择的顺序。"""
        raw_ids = self._first_present(
            values, "window_ids", "windowIds", "窗口ID列表", "窗口id列表", "窗口列表", "窗口id"
        )
        raw_codes = self._first_present(
            values, "window_codes", "windowCodes", "窗口编号列表", "窗口编号", "成员窗口"
        )
        ids: list[int] = []
        codes: list[str] = []
        if isinstance(raw_ids, (list, tuple)):
            ids = [_as_int(item) for item in raw_ids if str(item).strip()]
        elif raw_ids not in (None, ""):
            ids = [_as_int(item) for item in str(raw_ids).replace("，", ",").split(",") if item.strip()]
        if isinstance(raw_codes, (list, tuple)):
            codes = [str(item).strip() for item in raw_codes if str(item).strip()]
        elif raw_codes not in (None, ""):
            codes = [item.strip() for item in str(raw_codes).replace("，", ",").split(",") if item.strip()]

        windows: list[dict[str, Any]] = []
        seen: set[int] = set()
        for window_id in ids:
            window = store.find("extract_window", window_id)
            if window is None:
                raise ValueError(f"抽取窗口 {window_id} 不存在")
            if window["id"] in seen:
                continue
            seen.add(window["id"])
            windows.append(window)
        for code in codes:
            matches = [r for r in store.rows("extract_window") if r.get("窗口编号") == code]
            if not matches:
                raise ValueError(f"抽取窗口 {code} 不存在")
            window = matches[0]
            if window["id"] in seen:
                continue
            seen.add(window["id"])
            windows.append(window)
        if not windows:
            raise ValueError("缺少窗口列表，至少选择一个相邻窗口")
        return windows

    def _target_from_values(self, values: dict[str, Any]) -> str:
        target = self._first_present(values, "target_code", "targetCode", "目标编号", "target")
        return str(target).strip() if target not in (None, "") else "TARG-0001"

    @staticmethod
    def _first_present(values: dict[str, Any], *names: str) -> Any:
        for name in names:
            if name in values and values[name] not in (None, "", []):
                return values[name]
        return None

    def _validate_adjacent(self, windows: list[dict[str, Any]]) -> None:
        """校验所选窗口属于同一源端且相邻，防止跨源或跳窗混批。

        相邻定义为：在该源端所有窗口按窗口起点（再按 id）排序后的序列中，
        所选窗口占据一段连续位置。这样既支持真实时间边界，也支持同一批次
        按 id 连续创建的窗口。
        """
        if not windows:
            raise ValueError("缺少窗口列表，至少选择一个相邻窗口")
        sources = {str(window.get("源端编号", "")) for window in windows}
        if len(sources) > 1:
            raise ValueError("所选窗口来自多个源端，只能对同一源端的相邻窗口成组提交")
        source_code = next(iter(sources))
        family = sorted(
            [r for r in store.rows("extract_window") if str(r.get("源端编号", "")) == source_code],
            key=lambda row: (str(row.get("窗口起点", "")), _as_int(row.get("id"))),
        )
        selected = {int(window["id"]) for window in windows}
        positions = [index for index, row in enumerate(family) if int(row["id"]) in selected]
        if len(positions) != len(selected):
            raise ValueError("所选窗口未全部登记，无法成组提交")
        if positions and positions[-1] - positions[0] + 1 != len(positions):
            raise ValueError("所选窗口不相邻，请选择连续的相邻窗口")

    def _build_manifest(self, windows: list[dict[str, Any]], target_code: str) -> dict[str, Any]:
        ordered = sorted(
            windows,
            key=lambda window: (str(window.get("窗口起点", "")), _as_int(window.get("id"))),
        )
        manifest_code = self._unique_manifest_code()
        members = [self._build_member(manifest_code, index, window) for index, window in enumerate(ordered)]
        ready_count = sum(1 for member in members if member["extraction_ready"])
        pending_count = len(members) - ready_count
        return {
            "manifest_id": 0,
            "manifest_code": manifest_code,
            "target_code": target_code,
            "window_codes": [member["window_code"] for member in members],
            "window_ids": [member["window_id"] for member in members],
            "window_range": f"{ordered[0]['窗口编号']}~{ordered[-1]['窗口编号']}",
            "status": "待发布",
            "delivery_status": "待发布",
            "effective_windows": ready_count,
            "delivered_windows": 0,
            "pending_windows": pending_count,
            "watermark": 0,
            "created_at": time.time(),
            "updated_at": time.time(),
            "members": members,
            "events": [],
        }

    def _unique_manifest_code(self) -> str:
        """生成不撞车的发布编号：同一秒内多次创建也各自独立，成员批次号随之唯一。"""
        base = f"RLSE-{time.strftime('%Y%m%d%H%M%S', time.localtime())}"
        candidate = base
        suffix = 0
        while (self._data_dir("releases") / f"{candidate}.json").exists():
            suffix += 1
            candidate = f"{base}-{suffix:02d}"
        return candidate

    def _build_member(self, manifest_code: str, index: int, window: dict[str, Any]) -> dict[str, Any]:
        rows = self._window_artifact_rows(window)
        ready = self._window_is_extracted(window, rows)
        declared = _as_int(window.get("抽取条数"), len(rows))
        window_id = _as_int(window.get("id"))
        transfer_batch = f"TRAN-{manifest_code}-{window_id}"
        staging_code = f"STAG-{transfer_batch}"
        return {
            "member_code": f"{manifest_code}-M{index + 1:02d}",
            "window_id": window_id,
            "window_code": str(window.get("窗口编号", "")),
            "source_code": str(window.get("源端编号", "")),
            "window_start": window.get("窗口起点"),
            "window_end": window.get("窗口终点"),
            "declared_rows": declared,
            "expected_rows": declared if declared > 0 else len(rows),
            "actual_rows": 0,
            "delivered_rows": 0,
            "delivery_count": 0,
            "attempts": 0,
            "delivery_status": "待交付" if ready else "待补齐",
            "arrival_status": "待抵达" if ready else "待补齐",
            "extraction_ready": ready,
            "failure_position": None,
            "last_failure_position": None,
            "recovered_from": None,
            "fault_injected": False,
            "transfer_batch": transfer_batch,
            "staging_code": staging_code,
            "reconcile_code": f"ARRI-{transfer_batch}",
            "last_error": "" if ready else "抽取尚未完成",
            "artifact_path": str(self._window_artifact_path(window)),
        }

    # ------------------------------------------------------------------
    # 发布 / 重试 / 补数
    # ------------------------------------------------------------------
    def publish(
        self,
        manifest_id: int,
        *,
        only_window: Any = None,
    ) -> dict[str, Any]:
        """执行成组发布；已交付成员跳过，失败成员按原窗口水位继续。

        该方法既能作为首次提交，也能作为值班人员的重试入口。重试时只处理
        清单中尚未交付的成员，已经交付的窗口不会再次写入，交付次数保持为 1。
        """
        row = store.find(self.MODULE, manifest_id)
        if row is None:
            return {"error": f"成组发布清单 {manifest_id} 不存在", "message": "发布清单不存在"}
        manifest = self._load_manifest(row.get("发布编号", ""))
        if manifest is None:
            manifest = self._manifest_from_row(row)
        if manifest.get("status") == "已撤回":
            return {"error": "发布清单已撤回，不能继续发布", "message": "发布清单已撤回"}

        manifest["status"] = "发布中"
        manifest["delivery_status"] = "发布中"
        processed: list[dict[str, Any]] = []
        skipped: list[str] = []

        for member in manifest["members"]:
            if only_window is not None and not self._window_matches(member, only_window):
                continue
            window = store.find("extract_window", _as_int(member.get("window_id")))
            if window is None:
                member["delivery_status"] = "待补齐"
                member["arrival_status"] = "待补齐"
                member["extraction_ready"] = False
                member["last_error"] = "抽取窗口不存在"
                continue

            rows = self._window_artifact_rows(window)
            declared = _as_int(window.get("抽取条数"), len(rows))
            member["declared_rows"] = declared
            member["expected_rows"] = declared if declared > 0 else len(rows)
            member["extraction_ready"] = self._window_is_extracted(window, rows)
            member["artifact_path"] = str(self._window_artifact_path(window))

            if not member["extraction_ready"]:
                member["delivery_status"] = "待补齐"
                member["arrival_status"] = "待补齐"
                member["actual_rows"] = len(self._read_jsonl(
                    self._data_dir("staging") / f"{member['staging_code']}.jsonl"
                )) if member.get("staging_code") else 0
                member["last_error"] = (
                    f"抽取未完成：产物 {len(rows)}/{member['expected_rows']} 行"
                )
                self._upsert_delivery_rows(
                    manifest, member, member["expected_rows"], member["actual_rows"]
                )
                processed.append(self._member_view(member))
                continue

            if member["delivery_status"] == "已交付":
                skipped.append(member["window_code"])
                processed.append(self._member_view(member))
                continue

            self._deliver_member(manifest, member, rows)
            processed.append(self._member_view(member))

        self._refresh_manifest(manifest)
        self._append_event(manifest, {
            "action": "publish",
            "status": manifest["status"],
            "delivered": self._delivered_codes(manifest),
            "pending": self._pending_codes(manifest),
            "skipped": skipped,
            "at": time.time(),
        })
        self._save_manifest(manifest)
        self._sync_row(manifest)

        return {
            "ok": True,
            "manifest_id": manifest["manifest_id"],
            "manifest_code": manifest["manifest_code"],
            "status": manifest["status"],
            "delivery_status": manifest["delivery_status"],
            "effective_windows": manifest["effective_windows"],
            "delivered_windows": manifest["delivered_windows"],
            "pending_windows": manifest["pending_windows"],
            "delivered": self._delivered_codes(manifest),
            "pending": self._pending_codes(manifest),
            "skipped": skipped,
            "members": processed,
            "scope": self._scope_from_manifest(manifest),
            "message": (
                "整组窗口已全部交付"
                if manifest["status"] == "已交付"
                else f"成组发布部分完成，待补齐窗口：{'、'.join(self._pending_codes(manifest)) or '无'}"
            ),
        }

    def retry(self, manifest_id: int, **kwargs: Any) -> dict[str, Any]:
        """按原申请重试整组任务，默认不再注入演练故障。"""
        return self.publish(manifest_id, **kwargs)

    retry_group = retry
    retry_manifest = retry

    def supplement_window(
        self,
        manifest_id: int,
        window_id: Any,
        record_count: int | None = None,
    ) -> dict[str, Any]:
        """单独补上故障窗口的数据，并刷新该成员在清单中的可交付水位。

        补数只针对原窗口本身，不触碰同组其他窗口，也不会清掉已经交付的成员。
        补完之后仍需重新提交原申请，由 ``publish`` 从该窗口的断点继续写入。
        """
        row = store.find(self.MODULE, manifest_id)
        if row is None:
            return {"error": f"成组发布清单 {manifest_id} 不存在", "message": "发布清单不存在"}
        manifest = self._load_manifest(row.get("发布编号", "")) or self._manifest_from_row(row)
        member = next(
            (item for item in manifest["members"] if self._window_matches(item, window_id)),
            None,
        )
        if member is None:
            return {"error": f"窗口 {window_id} 不在发布清单中", "message": "窗口不属于该发布清单"}

        window = store.find("extract_window", _as_int(member["window_id"]))
        if window is None:
            return {"error": f"抽取窗口 {member['window_id']} 不存在", "message": "抽取窗口不存在"}

        from app.services.extract_window import ExtractWindowService

        extract_svc = ExtractWindowService()
        supplement = extract_svc.supplement_extract(
            _as_int(member["window_id"]),
            record_count=record_count or _as_int(member.get("declared_rows")) or None,
        )
        if supplement.get("error"):
            return supplement

        rows = self._window_artifact_rows(window)
        declared = _as_int(window.get("抽取条数"), len(rows))
        member["declared_rows"] = declared
        member["expected_rows"] = declared if declared > 0 else len(rows)
        member["extraction_ready"] = self._window_is_extracted(window, rows)
        member["last_error"] = ""
        if member["delivery_status"] != "已交付":
            member["delivery_status"] = "待交付" if member["extraction_ready"] else "待补齐"
            member["arrival_status"] = "待抵达" if member["extraction_ready"] else "待补齐"
        member["supplemented_at"] = time.time()

        self._refresh_manifest(manifest)
        self._append_event(manifest, {
            "action": "supplement",
            "window_code": member["window_code"],
            "expected_rows": member["expected_rows"],
            "at": time.time(),
        })
        self._save_manifest(manifest)
        self._sync_row(manifest)
        return {
            "ok": True,
            "manifest_id": manifest_id,
            "manifest_code": manifest["manifest_code"],
            "window_code": member["window_code"],
            "expected_rows": member["expected_rows"],
            "extraction_ready": member["extraction_ready"],
            "delivery_status": member["delivery_status"],
            "supplement": supplement,
            "message": f"窗口 {member['window_code']} 数据已补，请重新提交原申请",
        }

    # ------------------------------------------------------------------
    # 成员交付实现
    # ------------------------------------------------------------------
    def _deliver_member(
        self,
        manifest: dict[str, Any],
        member: dict[str, Any],
        artifact_rows: list[dict[str, Any]],
    ) -> None:
        """按窗口写入传输/暂存产物；已存在的行按行号幂等跳过，重试不重复写入。"""
        manifest_code = manifest["manifest_code"]
        window_code = member["window_code"]
        window_id = member["window_id"]
        expected = _as_int(member.get("expected_rows")) or len(artifact_rows)

        batch_code = member.get("transfer_batch") or f"TRAN-{manifest_code}-{window_id}"
        staging_code = member.get("staging_code") or f"STAG-{batch_code}"
        transfer_path = self._data_dir("batches") / f"{batch_code}.jsonl"
        staging_path = self._data_dir("staging") / f"{staging_code}.jsonl"
        member["transfer_batch"] = batch_code
        member["staging_code"] = staging_code
        member["reconcile_code"] = member.get("reconcile_code") or f"ARRI-{batch_code}"

        existing_transfer = self._read_jsonl(transfer_path)
        existing_staging = self._read_jsonl(staging_path)
        transfer_keys = {self._row_key(record) for record in existing_transfer}
        staging_keys = {self._row_key(record) for record in existing_staging}

        records: list[dict[str, Any]] = []
        for index, source_row in enumerate(artifact_rows):
            record = dict(source_row)
            record["window_id"] = window_id
            record["window_code"] = window_code
            record["manifest_code"] = manifest_code
            record["line_no"] = index + 1
            record["row_no"] = index + 1
            records.append(record)

        pending_transfer = [record for record in records if self._row_key(record) not in transfer_keys]
        pending_staging = [record for record in records if self._row_key(record) not in staging_keys]

        member["attempts"] = _as_int(member.get("attempts")) + 1

        # 正常写入：传输与暂存都按行号去重后追加
        if pending_transfer:
            self._write_jsonl(transfer_path, pending_transfer)
        if pending_staging:
            self._write_jsonl(staging_path, pending_staging)

        actual = len(self._read_jsonl(staging_path))
        member["actual_rows"] = actual
        member["delivered_rows"] = actual
        transfer_actual = len(self._read_jsonl(transfer_path))

        complete = expected > 0 and actual >= expected and transfer_actual >= expected
        if complete:
            if _as_int(member.get("delivery_count")) == 0:
                member["delivery_count"] = 1
            member["delivery_status"] = "已交付"
            member["arrival_status"] = "已抵达"
            member["last_error"] = ""
            member["delivered_at"] = time.time()
        else:
            member["delivery_status"] = "待补齐"
            member["arrival_status"] = "待补齐"
            member["last_error"] = f"窗口 {window_code} 仅写入 {actual}/{expected} 行"

        self._upsert_delivery_rows(manifest, member, expected, actual)

    def _upsert_delivery_rows(
        self,
        manifest: dict[str, Any],
        member: dict[str, Any],
        expected: int,
        actual: int,
    ) -> None:
        """同步传输批次、暂存分区、对账单三张表，保持与发布清单一致。"""
        manifest_code = manifest["manifest_code"]
        window_code = member["window_code"]
        delivered = member.get("delivery_status") == "已交付"
        missing = max(0, expected - actual)

        transfer_rows = self._ensure_row(
            "transfer_batch",
            manifest_code,
            window_code,
            {
                "批次编号": member["transfer_batch"],
                "窗口编号": window_code,
                "目标编号": manifest.get("target_code", "TARG-0001"),
                "记录条数": str(actual),
                "字节总量": "0",
                "分片大小": "1000",
                "传输状态": "已完成" if delivered else "传输中",
                "批次水位": str(actual),
            },
        )
        transfer_rows["记录条数"] = str(actual)
        transfer_rows["批次水位"] = str(actual)
        transfer_rows["传输状态"] = "已完成" if delivered else "传输中"
        transfer_rows["交付次数"] = str(member.get("delivery_count", 0))
        transfer_rows["实际行数"] = str(actual)
        transfer_rows["到达状态"] = member.get("arrival_status", "")
        transfer_rows["status"] = "已完成" if delivered else "传输中"
        transfer_rows["pending"] = not delivered
        transfer_rows["abnormal"] = not delivered and actual > 0

        staging_rows = self._ensure_row(
            "staging_area",
            manifest_code,
            window_code,
            {
                "暂存编号": member["staging_code"],
                "批次编号": member["transfer_batch"],
                "落地路径": f"/staging/{member['transfer_batch']}/",
                "文件数量": "1",
                "记录条数": str(actual),
                "占用字节": "0",
                "驻留时限": "86400",
                "落地水位": str(actual),
            },
        )
        staging_rows["记录条数"] = str(actual)
        staging_rows["落地水位"] = str(actual)
        staging_rows["实际行数"] = str(actual)
        staging_rows["到达状态"] = member.get("arrival_status", "")
        staging_rows["交付次数"] = str(member.get("delivery_count", 0))
        staging_rows["status"] = "已落地" if delivered else "落地中"
        staging_rows["pending"] = not delivered
        staging_rows["abnormal"] = not delivered and actual > 0

        reconcile_rows = self._ensure_row(
            "arrival_reconcile",
            manifest_code,
            window_code,
            {
                "对账编号": member["reconcile_code"],
                "批次编号": member["transfer_batch"],
                "应到条数": str(expected),
                "实到条数": "0",
                "迟到条数": "0",
                "缺失条数": str(expected),
                "对账窗口": "成组发布",
                "对账结论": "待对账",
            },
        )
        reconcile_rows["应到条数"] = str(expected)
        reconcile_rows["实到条数"] = str(actual)
        reconcile_rows["迟到条数"] = "0"
        reconcile_rows["缺失条数"] = str(missing)
        reconcile_rows["对账结论"] = "已平账" if delivered else ("存在差异" if actual else "待对账")
        reconcile_rows["到达状态"] = member.get("arrival_status", "")
        reconcile_rows["实际行数"] = str(actual)
        reconcile_rows["交付次数"] = str(member.get("delivery_count", 0))
        reconcile_rows["窗口编号"] = window_code
        reconcile_rows["status"] = "已平账" if delivered else ("存在差异" if actual else "待对账")
        reconcile_rows["pending"] = not delivered
        reconcile_rows["abnormal"] = not delivered and actual > 0

    def _ensure_row(
        self,
        module: str,
        manifest_code: str,
        window_code: str,
        defaults: dict[str, Any],
    ) -> dict[str, Any]:
        rows = store.rows(module)
        for row in rows:
            if row.get("发布编号") == manifest_code and row.get("窗口编号") == window_code:
                return row
        row = {"id": _next_id(rows)}
        row.update(defaults)
        row["发布编号"] = manifest_code
        row["窗口编号"] = window_code
        row["pending"] = True
        row["abnormal"] = False
        rows.append(row)
        return row

    # ------------------------------------------------------------------
    # 清单读取与范围查询
    # ------------------------------------------------------------------
    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        row = store.find(self.MODULE, entry_id)
        if row is None:
            return None
        manifest = self._load_manifest(row.get("发布编号", ""))
        if manifest:
            self._sync_row(manifest)
            row = store.find(self.MODULE, entry_id) or row
            row["members"] = manifest["members"]
            delivered = self._delivered_codes(manifest)
            pending = self._pending_codes(manifest)
            row["delivered_windows"] = delivered
            row["pending_windows"] = pending
            row["arrived_windows"] = delivered
            row["scope"] = self._scope_from_manifest(manifest)
        else:
            row["members"] = row.get("members", [])
            row["delivered_windows"] = []
            row["pending_windows"] = row.get("pending_windows", [])
            row["arrived_windows"] = []
        return row

    def get_manifest(self, manifest_code: str) -> dict[str, Any] | None:
        return self._load_manifest(manifest_code)

    def manifest_scope(self, manifest_code: str) -> dict[str, Any]:
        """返回发布清单的已抵达/待补齐窗口范围，供传输、暂存与对账共用。"""
        manifest = self._load_manifest(manifest_code)
        if manifest is None:
            return {"error": f"发布清单 {manifest_code} 不存在", "message": "发布清单不存在"}
        return self._scope_from_manifest(manifest)

    delivered_scope = manifest_scope
    get_manifest_scope = manifest_scope

    def list_manifests(self, *, window_code: str | None = None) -> list[dict[str, Any]]:
        """列出清单；可按成员窗口编号反查所属成组申请。"""
        manifests = [self._load_manifest(row.get("发布编号", "")) for row in store.rows(self.MODULE)]
        result = [item for item in manifests if item]
        if window_code:
            result = [
                item for item in result
                if any(member.get("window_code") == window_code for member in item.get("members", []))
            ]
        return result

    # ------------------------------------------------------------------
    # 动作接口
    # ------------------------------------------------------------------
    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        normalized = action.strip()
        if normalized in ("发布成组批次", "启动发布", "提交发布", "提交成组批次"):
            result = self.publish(entry_id)
            entry = store.find(self.MODULE, entry_id)
            if result.get("error"):
                return None, result["message"]
            return entry, result.get("message", "成组发布已执行")
        if normalized in ("重试发布", "重试成组批次", "继续发布"):
            result = self.retry(entry_id)
            entry = store.find(self.MODULE, entry_id)
            if result.get("error"):
                return None, result["message"]
            return entry, result.get("message", "成组发布已重试")
        if normalized in ("确认交付", "确认发布"):
            row = self.get_entry(entry_id)
            if row is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            if row.get("pending_windows"):
                return None, f"仍有待补齐窗口：{'、'.join(row['pending_windows'])}"
            entry, message = super().run_action(entry_id, "确认交付")
            return entry, message
        return super().run_action(entry_id, normalized)

    # ------------------------------------------------------------------
    # 清单持久化与状态刷新
    # ------------------------------------------------------------------
    def _register_manifest(self, manifest: dict[str, Any]) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        manifest["manifest_id"] = _next_id(rows)
        row = {
            "id": manifest["manifest_id"],
            "发布编号": manifest["manifest_code"],
            "窗口范围": manifest["window_range"],
            "窗口数量": str(len(manifest["members"])),
            "目标编号": manifest["target_code"],
            "有效窗口数": str(manifest["effective_windows"]),
            "已交付窗口数": "0",
            "待补齐窗口数": str(manifest["pending_windows"]),
            "交付状态": manifest["delivery_status"],
            "发布水位": "0",
            "status": manifest["status"],
            "pending": True,
            "abnormal": False,
            "manifest_code": manifest["manifest_code"],
            "window_ids": manifest["window_ids"],
            "members": manifest["members"],
            "delivered_windows": [],
            "pending_windows": manifest["window_codes"],
        }
        rows.append(row)
        self._append_event(manifest, {
            "action": "create_manifest",
            "window_codes": manifest["window_codes"],
            "target_code": manifest["target_code"],
            "at": time.time(),
        })
        self._save_manifest(manifest)
        return row

    def _sync_row(self, manifest: dict[str, Any]) -> None:
        row = store.find(self.MODULE, manifest["manifest_id"])
        if row is None:
            for candidate in store.rows(self.MODULE):
                if candidate.get("发布编号") == manifest["manifest_code"]:
                    row = candidate
                    break
        if row is None:
            return
        row["发布编号"] = manifest["manifest_code"]
        row["窗口范围"] = manifest["window_range"]
        row["窗口数量"] = str(len(manifest["members"]))
        row["目标编号"] = manifest["target_code"]
        row["有效窗口数"] = str(manifest["effective_windows"])
        row["已交付窗口数"] = str(manifest["delivered_windows"])
        row["待补齐窗口数"] = str(manifest["pending_windows"])
        row["交付状态"] = manifest["delivery_status"]
        row["发布水位"] = str(manifest["watermark"])
        row["status"] = manifest["status"]
        row["pending"] = manifest["status"] != "已交付"
        row["abnormal"] = manifest["status"] == "部分交付"
        row["members"] = manifest["members"]
        row["window_ids"] = manifest["window_ids"]
        row["delivered_windows"] = self._delivered_codes(manifest)
        row["pending_windows"] = self._pending_codes(manifest)

    def _refresh_manifest(self, manifest: dict[str, Any]) -> None:
        members = manifest["members"]
        delivered = [member for member in members if member.get("delivery_status") == "已交付"]
        pending = [member for member in members if member.get("delivery_status") != "已交付"]
        manifest["delivered_windows"] = len(delivered)
        manifest["pending_windows"] = len(pending)
        manifest["effective_windows"] = sum(1 for member in members if member.get("extraction_ready"))
        manifest["watermark"] = sum(_as_int(member.get("actual_rows")) for member in members)
        if len(delivered) == len(members) and members:
            manifest["status"] = "已交付"
            manifest["delivery_status"] = "已交付"
        elif delivered:
            manifest["status"] = "部分交付"
            manifest["delivery_status"] = "部分交付"
        elif any(member.get("delivery_status") in ("部分失败", "待补齐") for member in members):
            manifest["status"] = "部分交付"
            manifest["delivery_status"] = "部分交付"
        else:
            manifest["status"] = "发布中"
            manifest["delivery_status"] = "发布中"
        manifest["updated_at"] = time.time()

    def _scope_from_manifest(self, manifest: dict[str, Any]) -> dict[str, Any]:
        arrived = self._delivered_codes(manifest)
        pending = self._pending_codes(manifest)
        return {
            "manifest_id": manifest["manifest_id"],
            "manifest_code": manifest["manifest_code"],
            "status": manifest["status"],
            "total_windows": len(manifest["members"]),
            "arrived_windows": arrived,
            "delivered_windows": arrived,
            "pending_windows": pending,
            "waiting_windows": pending,
            "members": [self._member_view(member) for member in manifest["members"]],
        }

    @staticmethod
    def _delivered_codes(manifest: dict[str, Any]) -> list[str]:
        return [
            member["window_code"] for member in manifest.get("members", [])
            if member.get("delivery_status") == "已交付"
        ]

    @staticmethod
    def _pending_codes(manifest: dict[str, Any]) -> list[str]:
        return [
            member["window_code"] for member in manifest.get("members", [])
            if member.get("delivery_status") != "已交付"
        ]

    @staticmethod
    def _member_view(member: dict[str, Any]) -> dict[str, Any]:
        return {
            "window_id": member.get("window_id"),
            "window_code": member.get("window_code"),
            "expected_rows": member.get("expected_rows", 0),
            "actual_rows": member.get("actual_rows", 0),
            "delivered_rows": member.get("delivered_rows", 0),
            "delivery_count": member.get("delivery_count", 0),
            "attempts": member.get("attempts", 0),
            "delivery_status": member.get("delivery_status"),
            "arrival_status": member.get("arrival_status"),
            "extraction_ready": member.get("extraction_ready"),
            "failure_position": member.get("failure_position"),
            "last_failure_position": member.get("last_failure_position"),
            "recovered_from": member.get("recovered_from"),
            "transfer_batch": member.get("transfer_batch"),
            "staging_code": member.get("staging_code"),
            "reconcile_code": member.get("reconcile_code"),
            "last_error": member.get("last_error", ""),
        }

    def _manifest_from_row(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "manifest_id": _as_int(row.get("id")),
            "manifest_code": row.get("发布编号", ""),
            "target_code": row.get("目标编号", "TARG-0001"),
            "window_codes": [member.get("window_code") for member in row.get("members", [])],
            "window_ids": row.get("window_ids", []),
            "window_range": row.get("窗口范围", ""),
            "status": row.get("status", "待发布"),
            "delivery_status": row.get("交付状态", "待发布"),
            "effective_windows": _as_int(row.get("有效窗口数")),
            "delivered_windows": _as_int(row.get("已交付窗口数")),
            "pending_windows": _as_int(row.get("待补齐窗口数")),
            "watermark": _as_int(row.get("发布水位")),
            "created_at": time.time(),
            "updated_at": time.time(),
            "members": list(row.get("members", [])),
            "events": [],
        }

    def _manifest_path(self, manifest_code: str) -> Path:
        return self._data_dir("releases") / f"{manifest_code}.json"

    def _load_manifest(self, manifest_code: str) -> dict[str, Any] | None:
        if not manifest_code:
            return None
        path = self._manifest_path(manifest_code)
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8") as handle:
            return json.load(handle)

    def _save_manifest(self, manifest: dict[str, Any]) -> None:
        path = self._manifest_path(manifest["manifest_code"])
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(manifest, handle, ensure_ascii=False, indent=2, default=str)

    @staticmethod
    def _append_event(manifest: dict[str, Any], event: dict[str, Any]) -> None:
        manifest.setdefault("events", []).append(event)

    # ------------------------------------------------------------------
    # 抽取产物读取
    # ------------------------------------------------------------------
    def _window_artifact_path(self, window: dict[str, Any]) -> Path:
        return self._data_dir("extracted") / f"{window.get('窗口编号', '')}.jsonl"

    def _window_artifact_rows(self, window: dict[str, Any]) -> list[dict[str, Any]]:
        return self._read_jsonl(self._window_artifact_path(window))

    @staticmethod
    def _window_is_extracted(window: dict[str, Any], rows: list[dict[str, Any]]) -> bool:
        """抽取完成才算可交付：窗口已闭合且产物行数达到声明的抽取条数。"""
        if window.get("status") in ("草稿", "待抽取", "已作废"):
            return False
        if window.get("status") != "已闭合":
            return False
        if not rows:
            return False
        declared = _as_int(window.get("抽取条数"))
        return declared <= 0 or len(rows) >= declared

    def _pick_window(
        self,
        windows: list[dict[str, Any]],
        target: Any,
    ) -> dict[str, Any] | None:
        """按窗口 id、窗口编号或组内序号定位演练目标窗口。"""
        ids = {_as_int(window.get("id")): window for window in windows}
        if isinstance(target, int) and target in ids:
            return ids[target]
        for window in windows:
            if str(target).strip() in (str(window.get("窗口编号")), str(window.get("id"))):
                return window
        if isinstance(target, int) and 1 <= target <= len(windows):
            return windows[target - 1]
        return None

    def _inject_extraction_fault(self, window: dict[str, Any], fail_after_rows: int | None) -> int:
        """模拟抽取写入中途失败：只保留前 N 行，窗口停在「抽取中」，返回失败行位。"""
        from app.services.extract_window import ExtractWindowService

        artifact = self._window_artifact_path(window)
        rows = self._read_jsonl(artifact)
        if not rows:
            expected = _as_int(window.get("抽取条数")) or 10
            rows = ExtractWindowService()._simulate_records(
                window.get("源端编号", ""), count=expected, field_prefix="ext"
            )
            for offset, record in enumerate(rows, start=1):
                record["line_no"] = offset
                record["row_no"] = offset
                record["window_code"] = window.get("窗口编号")
        limit = fail_after_rows if fail_after_rows and fail_after_rows > 0 else max(1, len(rows) // 2)
        limit = min(limit, max(0, len(rows) - 1))
        partial = rows[:limit]
        with open(artifact, "w", encoding="utf-8") as handle:
            for record in partial:
                handle.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        window["status"] = "抽取中"
        window["pending"] = True
        window["推进水位"] = str(len(partial))
        self._write_jsonl(
            self._data_dir("events") / "window_extract_fail_log.jsonl",
            [{"action": "extract_fault", "window_code": window.get("窗口编号"),
              "written_rows": len(partial), "declared_rows": len(rows), "at": time.time()}],
        )
        return len(partial)

    def _extract_window(self, window: dict[str, Any]) -> None:
        from app.services.extract_window import ExtractWindowService

        expected = _as_int(window.get("抽取条数")) or 10
        ExtractWindowService().execute_extract(_as_int(window.get("id")), record_count=expected)

    @staticmethod
    def _row_key(record: dict[str, Any]) -> tuple[Any, Any]:
        return (record.get("window_code"), record.get("line_no", record.get("row_no")))

    @staticmethod
    def _window_matches(member: dict[str, Any], window: Any) -> bool:
        """成员是否对应用户指定的窗口（编号或 id 均可）。"""
        if window in (None, ""):
            return False
        if isinstance(window, int):
            return window == _as_int(member.get("window_id"))
        text = str(window).strip()
        return text == str(member.get("window_code")) or text == str(member.get("window_id"))
