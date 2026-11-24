"""到达对账领域服务：维护批次应到与实到记录数的对账、迟到与缺失条数统计及结论。

本模块读取传输批次产物文件和暂存落地产物文件进行数量对账，统计迟到与缺失条数，
对账完成后自动驱动时效监控指标更新和传输血缘记录。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ArrivalReconcileService(BaseModuleService):
    MODULE = "arrival_reconcile"
    ENTITY = "对账单"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["对账编号", "批次编号", "应到条数", "实到条数", "迟到条数", "缺失条数", "对账窗口", "对账结论"]
    REQUIRED_FIELDS = ["对账编号", "批次编号", "应到条数"]
    STATUS_ORDER = ["草稿", "待对账", "对账中", "已平账", "存在差异"]
    ACTION_RULES = {"启动对账": "对账中", "确认平账": "已平账", "标记差异": "存在差异"}
    NEGATIVE_ACTIONS = ["标记差异"]

    def create_reconciliation(self, batch_code: str, expected_count: int) -> dict[str, Any]:
        """为传输批次创建到达对账单，登记应到条数，为后续到账核对准备基线。"""
        recon_code = f"ARRI-{batch_code}-{int(time.time())}"
        values = {
            "对账编号": recon_code,
            "批次编号": batch_code,
            "应到条数": str(expected_count),
            "实到条数": "0",
            "迟到条数": "0",
            "缺失条数": "0",
            "对账窗口": "daily",
            "对账结论": "待对账",
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            raise ValueError(f"创建对账单失败: {missing}")
        return {"recon_code": recon_code, "recon_id": entry["id"], "batch_code": batch_code,
                "expected_count": expected_count}

    def reconcile_manifest(self, manifest_code: str) -> dict[str, Any]:
        """据成组发布清单执行整组对账，区分已抵达窗口与尚待补齐窗口。

        对账以发布清单为唯一事实来源：逐窗口比对清单应到行数与暂存实际落地产物，
        已交付且行数齐的窗口判为已抵达，其余判为待补齐，并回写每个成员的对账值。
        """
        from app.services.release_manifest import ReleaseManifestService
        from app.services.staging_area import StagingAreaService

        release_svc = ReleaseManifestService()
        manifest = release_svc.get_manifest(manifest_code)
        if manifest is None:
            return {"error": f"发布清单 {manifest_code} 不存在", "message": "发布清单不存在"}

        staging_scope = StagingAreaService().staging_scope(manifest_code)
        staging_by_window = {
            item["window_code"]: item for item in staging_scope.get("members", [])
        }
        rows = store.rows(self.MODULE)
        arrived, waiting, member_results = [], [], []
        for member in manifest.get("members", []):
            window_code = member.get("window_code")
            expected = int(member.get("expected_rows") or 0)
            staging = staging_by_window.get(window_code, {})
            actual = int(staging.get("staging_rows") or 0)
            missing = max(0, expected - actual)
            balanced = (
                member.get("delivery_status") == "已交付"
                and expected > 0
                and actual >= expected
            )
            recon_code = member.get("reconcile_code") or f"ARRI-{member.get('transfer_batch')}"
            recon_row = next((row for row in rows if row.get("对账编号") == recon_code), None)
            if recon_row is None and recon_code:
                recon_row = {
                    "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
                    "对账编号": recon_code,
                    "批次编号": member.get("transfer_batch"),
                    "对账窗口": "成组发布",
                    "pending": True,
                    "abnormal": False,
                }
                rows.append(recon_row)
            if recon_row is not None:
                recon_row.update({
                    "应到条数": str(expected),
                    "实到条数": str(actual),
                    "迟到条数": "0",
                    "缺失条数": str(missing),
                    "对账结论": "已平账" if balanced else "待补齐",
                    "到达状态": "已抵达" if balanced else "待补齐",
                    "实际行数": str(actual),
                    "交付次数": str(member.get("delivery_count", 0)),
                    "发布编号": manifest_code,
                    "窗口编号": window_code,
                    "status": "已平账" if balanced else "待对账",
                    "pending": not balanced,
                    "abnormal": not balanced and actual > 0,
                })
            result = {
                "window_code": window_code,
                "expected_rows": expected,
                "actual_rows": actual,
                "missing_rows": missing,
                "delivery_count": member.get("delivery_count", 0),
                "arrival_status": "已抵达" if balanced else "待补齐",
                "reconcile_code": recon_code,
            }
            member_results.append(result)
            (arrived if balanced else waiting).append(window_code)

        self._write_jsonl(
            self._data_dir("events") / "reconcile_log.jsonl",
            [{"action": "reconcile_manifest", "manifest_code": manifest_code,
              "arrived": arrived, "waiting": waiting, "at": time.time()}],
        )
        return {
            "ok": True,
            "manifest_code": manifest_code,
            "arrived_windows": arrived,
            "waiting_windows": waiting,
            "delivered_windows": arrived,
            "pending_windows": waiting,
            "members": member_results,
            "message": f"已抵达 {len(arrived)} 窗，待补齐 {len(waiting)} 窗",
        }

    reconcile_group = reconcile_manifest

    def execute_reconciliation(self, recon_id: int) -> dict[str, Any]:
        """执行到达对账：核实传输批次应到与实到记录数的一致性，统计迟到与缺失条数。

        本方法从传输批次产物文件获取应到条数，从暂存落地产物文件获取实到条数，
        计算迟到（超时到达）和缺失（未到达）条数，判定对账结论。
        """
        recon_entry = self.get_entry(recon_id)
        if recon_entry is None:
            return {"error": f"对账单 {recon_id} 不存在"}

        entry, msg = self.run_action(recon_id, "启动对账")
        if entry is None:
            return {"error": msg, "recon_id": recon_id}

        batch_code = recon_entry["批次编号"]
        expected = int(recon_entry.get("应到条数", 0))

        # 从传输批次产物文件读取源数据
        batch_file = self._data_dir("batches") / f"{batch_code}.jsonl"
        batch_records = self._read_jsonl(batch_file)
        source_count = len(batch_records)

        # 从暂存产品目录查找目标落地产物文件
        staging_dir = self._data_dir("staging")
        staging_count = 0
        late_count = 0
        for sf in staging_dir.glob(f"*{batch_code}*.jsonl"):
            s_records = self._read_jsonl(sf)
            staging_count = len(s_records)
            # 计算迟到条数：目标端实际记录数少于源端基准
            if staging_count < expected:
                late_count = expected - staging_count
            break

        missing_count = max(0, expected - staging_count)
        is_balanced = expected == staging_count

        entry["实到条数"] = str(staging_count)
        entry["迟到条数"] = str(late_count)
        entry["缺失条数"] = str(missing_count)

        result_action = "确认平账" if is_balanced else "标记差异"
        final_entry, _ = self.run_action(recon_id, result_action)

        # 写入对账结论日志
        self._write_jsonl(
            self._data_dir("events") / "reconcile_log.jsonl",
            [{"action": "reconcile", "batch_code": batch_code, "expected": expected,
              "actual": staging_count, "late": late_count, "missing": missing_count,
              "balanced": is_balanced, "reconciled_at": time.time()}]
        )

        result = {
            "recon_code": recon_entry["对账编号"],
            "batch_code": batch_code,
            "expected": expected,
            "actual": staging_count,
            "late": late_count,
            "missing": missing_count,
            "balanced": is_balanced,
        }

        # 对账结果驱动时效监控指标更新
        if late_count > 0 or not is_balanced:
            try:
                sm_svc = self._safe_import("app.services.sla_monitor", "SlaMonitorService")()
                sm_result = sm_svc.update_arrival_stats(batch_code, late_count, missing_count)
                result["auto_sla_update"] = sm_result
            except Exception as e:
                result["auto_sla_update"] = {"error": str(e)}

        # 对账完成后记录传输血缘
        try:
            tl_svc = self._safe_import("app.services.transfer_lineage", "TransferLineageService")()
            lineage_result = tl_svc.record_lineage(batch_code, is_balanced)
            result["auto_lineage"] = lineage_result
        except Exception as e:
            result["auto_lineage"] = {"error": str(e)}

        return result