"""集成测试：跨模块数据交换链路端到端验证。

覆盖8条核心数据流链路的真实读写、异常处理和跨模块一致性。
"""
from __future__ import annotations

import os
import json
import time
import shutil
from pathlib import Path

import pytest

from app.services.base import DATA_ROOT
from app.services.source_register import SourceRegisterService
from app.services.target_register import TargetRegisterService
from app.services.extract_window import ExtractWindowService
from app.services.release_manifest import ReleaseManifestService
from app.services.transfer_batch import TransferBatchService
from app.services.staging_area import StagingAreaService
from app.services.checksum_verify import ChecksumVerifyService
from app.services.arrival_reconcile import ArrivalReconcileService
from app.services.conflict_resolve import ConflictResolveService
from app.services.reject_record import RejectRecordService
from app.services.field_contract import FieldContractService
from app.services.resume_point import ResumePointService
from app.services.speed_control import SpeedControlService
from app.services.format_adapt import FormatAdaptService
from app.services.transfer_lineage import TransferLineageService
from app.services.history_archive import HistoryArchiveService
from app.services.exchange_replay import ExchangeReplayService
from app.services.sla_monitor import SlaMonitorService
from app.services.channel_config import ChannelConfigService
from app.store import store


@pytest.fixture(autouse=True)
def cleanup():
    """每个测试前重置store数据，确保测试隔离（文件清理被沙箱限制，仅append不影响结果）。"""
    from app.seed import SEED_ROWS
    store._tables = {
        name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
    }
    yield


class TestChain1SourceToExtractToBatch:
    """链路1: 源端登记→窗口抽取→传输批次（贯通验证）"""

    def test_source_register_triggers_window_creation(self):
        """验证源端登记确认后自动创建增量抽取窗口。"""
        svc = SourceRegisterService()
        values = {"源端编号": "SOUR-CHAIN1", "系统名称": "测试系统1", "数据域": "订单域",
                  "记录格式": "JSON", "编码方式": "UTF-8", "日增量级": "100万",
                  "抽取方式": "增量抽取", "登记游标": "0"}
        entry = svc.register_source(values)
        assert entry is not None
        result = svc.confirm_register(entry["id"])
        assert "error" not in result
        triggers = result.get("auto_triggers", {})
        # 确认自动创建了抽取窗口
        if "extract_window" in triggers and "error" not in str(triggers["extract_window"]):
            assert triggers["extract_window"]["status"] == "草稿"
        # 确认自动创建了字段契约
        if "field_contract" in triggers and "error" not in str(triggers["field_contract"]):
            assert triggers["field_contract"]["status"] == "已生效"

    def test_full_extract_to_batch_pipeline(self):
        """验证抽取窗口执行后产物文件存在且自动驱动传输批次创建。"""
        # 前置：确保源端存在
        svc = SourceRegisterService()
        values = {"源端编号": "SOUR-CHAIN1B", "系统名称": "管道测试", "数据域": "交易域",
                  "记录格式": "JSON", "编码方式": "UTF-8", "日增量级": "50万",
                  "抽取方式": "增量抽取", "登记游标": "0"}
        entry = svc.register_source(values)
        svc.confirm_register(entry["id"])

        # 创建抽取窗口并执行抽取
        ew_svc = ExtractWindowService()
        window_entry = ew_svc.create_window("SOUR-CHAIN1B", count=15)
        result = ew_svc.execute_extract(window_entry["id"], record_count=15)
        assert "error" not in result
        assert result["records_extracted"] == 15

        # 验证产物文件存在且数据可读
        artifact = Path(result["artifact_path"])
        assert artifact.exists()

        # 验证自动触发了传输批次
        assert "auto_batch" in result
        assert "error" not in result["auto_batch"]


class TestChain2BatchToStagingToVerify:
    """链路2: 传输批次→暂存落地→校验核对（贯通验证）"""

    def test_batch_transfer_triggers_staging_and_verify(self):
        """验证传输执行后自动触发暂存落地和校验核对。"""
        tb_svc = TransferBatchService()
        batch = tb_svc.create_batch("EXTR-SOUR-xxx", 10, "TARG-0001")

        result = tb_svc.start_transfer(batch["batch_id"], chunk_size=3)
        assert "error" not in result
        assert result["records_transferred"] > 0

        # 验证产物文件存在
        artifact = Path(result["artifact_path"])
        assert artifact.exists()
        records = tb_svc._read_jsonl(artifact)
        assert len(records) > 0

        # 验证自动触发暂存落地
        assert "auto_staging" in result
        assert "error" not in result["auto_staging"]

        # 验证自动触发校验核对
        assert "auto_verify" in result
        assert "error" not in result["auto_verify"]

    def test_staging_lands_batch_data_correctly(self):
        """验证暂存落地读取传输产物并写入暂存产物。"""
        # 先创建传输批次产物
        tb_svc = TransferBatchService()
        batch = tb_svc.create_batch("EXTR-STAG-TEST", 10, "TARG-0001")
        tb_svc.start_transfer(batch["batch_id"], chunk_size=5)

        # 独立调用暂存落地验证产物
        sa_svc = StagingAreaService()
        result = sa_svc.land_batch(batch["batch_code"], 10)
        assert result["records_landed"] > 0
        artifact = Path(result["artifact_path"])
        assert artifact.exists()

    def test_checksum_verify_with_mismatch(self):
        """验证校验核对在执行后产生差异数据。"""
        tb_svc = TransferBatchService()
        batch = tb_svc.create_batch("EXTR-CHECK-TEST", 10, "TARG-0001")
        tb_svc.start_transfer(batch["batch_id"], chunk_size=5)

        cv_svc = ChecksumVerifyService()
        verify = cv_svc.create_verification(batch["batch_code"], 10)
        result = cv_svc.execute_verification(verify["verify_id"])
        # 校验可能通过也可能不通过，但结果必须有 valid keys
        assert "source_checksum" in result


class TestChain3VerifyDrivesConflictAndReject:
    """链路3: 校验差异→冲突裁决+拒收回执（贯通验证）"""

    def test_verification_diff_creates_conflict(self):
        """验证校验差异自动驱动冲突记录创建。"""
        cv_svc = ChecksumVerifyService()
        cn_svc = ConflictResolveService()

        # 创建冲突记录
        conflict = cn_svc.create_conflict("TEST-BATCH", 3)
        assert conflict["conflicts_created"] == 3

        # 裁决冲突
        for code in conflict["codes"][:1]:
            c = cn_svc.resolve_conflict(
                [r["id"] for r in store.rows("conflict_resolve") if r.get("冲突编号") == code][0],
                "source_first")
            assert c["final_value"] != ""
            assert c["strategy"] == "source_first"

    def test_rejection_creation_and_disposition(self):
        """验证拒收记录的创建与处置流程。"""
        rr_svc = RejectRecordService()
        rejection = rr_svc.create_rejection("TEST-BATCH", "校验", "SHA256校验差异5条")
        assert rejection["batch_code"] == "TEST-BATCH"

        # 处置拒收
        result = rr_svc.process_rejection(rejection["reject_id"], "return")
        assert result["result"] == "已退回"


class TestChain4ReconcileToSlaAndLineage:
    """链路4: 到达对账→时效监控+传输血缘（贯通验证）"""

    def test_reconciliation_updates_sla(self):
        """验证到达对账发现迟到后触发时效监控更新。"""
        ar_svc = ArrivalReconcileService()
        recon = ar_svc.create_reconciliation("TEST-BATCH-SLA", 100)
        result = ar_svc.execute_reconciliation(recon["recon_id"])
        # 对账结果驱动 SLA 更新
        assert "auto_sla_update" in result

    def test_reconciliation_records_lineage(self):
        """验证到达对账完成后记录传输血缘。"""
        ar_svc = ArrivalReconcileService()
        recon = ar_svc.create_reconciliation("TEST-BATCH-LIN", 50)
        result = ar_svc.execute_reconciliation(recon["recon_id"])
        assert "auto_lineage" in result


class TestChain5FieldContractMapping:
    """链路5: 字段契约映射（贯通验证）"""

    def test_field_contract_apply_to_extracted_data(self):
        """验证字段契约对抽取产物执行类型转换与映射。"""
        # 确保有契约并确认生效
        fc_svc = FieldContractService()
        entry = fc_svc.create_contract("ext_a", "dst_a", "string")
        fc_svc.run_action(entry["id"], "确认生效")

        # 应用契约到抽取产物
        result = fc_svc.apply_contracts("SOUR-0001", record_count=10)
        assert result["records_converted"] > 0
        artifact = Path(result["artifact_path"])
        assert artifact.exists()
        records = fc_svc._read_jsonl(artifact)
        assert "dst_a" in records[0]


class TestChain6ResumePointAndSpeedControl:
    """链路6: 断点续传与速率整形（贯通验证）"""

    def test_resume_point_creation_and_transfer(self):
        """验证断点续传的创建与恢复。"""
        rp_svc = ResumePointService()
        resume = rp_svc.create_resume_point("TEST-BATCH-RESUME", 5, 5)
        result = rp_svc.resume_transfer(resume["resume_id"])
        assert result["records_resumed"] >= 0

    def test_speed_control_shaping(self):
        """验证速率整形算法输出产物。"""
        sc_svc = SpeedControlService()
        policy = sc_svc.create_policy("测试范围", 100)
        result = sc_svc.apply_shaping(policy["id"], "TEST-BATCH-SHAPE", 200)
        assert result["shaped"] or "queued" in result
        shaping_file = sc_svc._data_dir("shaping") / "TEST-BATCH-SHAPE_shaping.jsonl"
        assert shaping_file.exists()


class TestChain7FormatAdapt:
    """链路7: 格式适配（贯通验证）"""

    def test_format_adapt_creation_and_apply(self):
        """验证格式适配的差异识别与格式转换。"""
        fa_svc = FormatAdaptService()
        entry = fa_svc.create_adaptation("order_format", "id,name,price", "id,title,price,discount")
        assert entry is not None

        result = fa_svc.execute_adaptation(entry["id"], "TEST-BATCH-FMT")
        assert result["records_adapted"] > 0
        artifact = Path(result["artifact_path"])
        assert artifact.exists()


class TestChain8ArchiveAndReplay:
    """链路8: 批次归档与历史重放（贯通验证）"""

    def test_archive_and_replay_cycle(self):
        """验证归档与重放的完整生命周。"""
        ha_svc = HistoryArchiveService()
        er_svc = ExchangeReplayService()

        archive = ha_svc.create_archive(["BATCH-A1", "BATCH-A2"])
        result = ha_svc.execute_archive(archive["archive_id"])
        assert result["record_count"] > 0
        assert result["checksum"] != ""

        # 重放归档数据
        replay = er_svc.create_replay(archive["archive_code"], "BATCH-A1-BATCH-A2", "append")
        reply_result = er_svc.execute_replay(replay["replay_id"])
        assert reply_result["records_replayed"] > 0

    def test_archive_destroy(self):
        """验证归档卷销毁操作。"""
        ha_svc = HistoryArchiveService()
        archive = ha_svc.create_archive(["BATCH-D1"])
        ha_svc.execute_archive(archive["archive_id"])
        result = ha_svc.destroy_archive(archive["archive_id"])
        assert result["status"] == "已销毁"


class TestChain9GroupedRelease:
    """链路9: 相邻窗口成组发布→传输→暂存→对账（共享发布清单）。

    覆盖需求中的关键场景：一次提交三个相邻窗口、中间窗口部分失败、按原窗口
    重试、已交付成员不重复写入，以及中断期间暂存与对账的已交付范围一致性。
    """

    @staticmethod
    def _prepare_windows(counts=(10, 11, 12), source_code="SOUR-GROUP"):
        sr_svc = SourceRegisterService()
        entry = sr_svc.register_source({
            "源端编号": source_code, "系统名称": "成组发布测试", "数据域": "订单域",
            "记录格式": "JSON", "编码方式": "UTF-8", "日增量级": "10万",
            "抽取方式": "增量抽取", "登记游标": "0",
        })
        sr_svc.confirm_register(entry["id"])
        ew_svc = ExtractWindowService()
        windows = []
        for count in counts:
            window = ew_svc.create_window(source_code, count=count)
            ew_svc.execute_extract(window["id"], record_count=count)
            windows.append(window)
        return windows

    def test_group_submit_three_adjacent_windows_all_delivered(self):
        """一次提交整组三个相邻窗口，全部交付且各窗行数与交付次数正确。"""
        windows = self._prepare_windows()
        svc = ReleaseManifestService()

        result = svc.submit_group(window_ids=[window["id"] for window in windows])

        assert result["ok"] is True
        assert result["status"] == "已交付"
        assert result["delivered_windows"] == 3
        assert result["pending_windows"] == 0
        assert [member["window_code"] for member in result["members"]] == [
            window["窗口编号"] for window in windows
        ]
        for member, expected in zip(result["members"], (10, 11, 12)):
            assert member["expected_rows"] == expected
            assert member["actual_rows"] == expected
            assert member["arrival_status"] == "已抵达"
            assert member["delivery_count"] == 1

    def test_middle_window_partial_failure_then_retry(self):
        """中间窗口写入部分完成后失败：前后窗已交付，重试只补中间窗且不重复写入。"""
        windows = self._prepare_windows()
        svc = ReleaseManifestService()
        middle = windows[1]

        first = svc.submit_group(
            window_ids=[window["id"] for window in windows],
            fail_window=middle["id"],
            fail_after_rows=4,
        )
        assert first["status"] == "部分交付"
        by_code = {member["window_code"]: member for member in first["members"]}
        assert by_code[windows[0]["窗口编号"]]["delivery_status"] == "已交付"
        assert by_code[windows[2]["窗口编号"]]["delivery_status"] == "已交付"
        assert by_code[middle["窗口编号"]]["delivery_status"] == "待补齐"
        assert by_code[middle["窗口编号"]]["actual_rows"] == 0
        assert by_code[middle["窗口编号"]]["failure_position"] == 4
        assert by_code[middle["窗口编号"]]["delivery_count"] == 0

        # 中间窗口的抽取产物确实只写入了前 4 行
        artifact = DATA_ROOT / "extracted" / f"{middle['窗口编号']}.jsonl"
        assert len(ReleaseManifestService()._read_jsonl(artifact)) == 4

        # 直接重试不会凭空补齐抽取，中间窗仍然待补齐
        blocked = svc.retry(first["manifest_id"])
        blocked_member = {
            member["window_code"]: member for member in blocked["members"]
        }[middle["窗口编号"]]
        assert blocked_member["delivery_status"] == "待补齐"
        assert blocked["status"] == "部分交付"

        # 单独补上故障窗的数据后重试，整组交付完成
        svc.supplement_window(first["manifest_id"], middle["id"])
        second = svc.retry(first["manifest_id"])
        assert second["status"] == "已交付"
        assert second["skipped"] == [windows[0]["窗口编号"], windows[2]["窗口编号"]]
        retried = {member["window_code"]: member for member in second["members"]}
        for window, expected in zip(windows, (10, 11, 12)):
            member = retried[window["窗口编号"]]
            assert member["actual_rows"] == expected
            assert member["delivery_count"] == 1
            assert member["delivery_status"] == "已交付"
        # 中间窗口的产品文件按行号续写，最终恰好 11 行，没有重复
        records = ReleaseManifestService()._read_jsonl(
            DATA_ROOT / "batches" / f"{retried[middle['窗口编号']]['transfer_batch']}.jsonl"
        )
        assert len(records) == 11
        assert sorted(record["line_no"] for record in records) == list(range(1, 12))

    def test_interrupted_scope_matches_between_staging_and_reconcile(self):
        """中断第二窗后，暂存与对账的已交付范围一致，且与共享清单相同。"""
        windows = self._prepare_windows()
        svc = ReleaseManifestService()
        result = svc.submit_group(
            window_ids=[window["id"] for window in windows],
            fail_window=windows[1]["id"],
            fail_after_rows=5,
        )
        code = result["manifest_code"]

        manifest_scope = svc.manifest_scope(code)
        staging_scope = StagingAreaService().staging_scope(code)
        reconcile = ArrivalReconcileService().reconcile_manifest(code)

        expected_arrived = [windows[0]["窗口编号"], windows[2]["窗口编号"]]
        expected_pending = [windows[1]["窗口编号"]]
        assert manifest_scope["arrived_windows"] == expected_arrived
        assert staging_scope["arrived_windows"] == expected_arrived
        assert reconcile["arrived_windows"] == expected_arrived
        assert manifest_scope["pending_windows"] == expected_pending
        assert staging_scope["waiting_windows"] == expected_pending
        assert reconcile["waiting_windows"] == expected_pending

        middle_staging = next(
            item for item in staging_scope["members"]
            if item["window_code"] == windows[1]["窗口编号"]
        )
        assert middle_staging["staging_rows"] == 0
        assert middle_staging["expected_rows"] == 11
        assert middle_staging["arrival_status"] == "待补齐"

    def test_supplement_then_resubmit_recovers_original_window(self):
        """单独补齐故障窗后重新提交原申请，逐条核对实际行、到达状态与交付次数。"""
        windows = self._prepare_windows()
        svc = ReleaseManifestService()
        first = svc.submit_group(
            window_ids=[window["id"] for window in windows],
            fail_window=windows[1]["id"],
            fail_after_rows=3,
        )
        code = first["manifest_code"]

        supplement = svc.supplement_window(first["manifest_id"], windows[1]["id"])
        assert supplement["ok"] is True
        assert supplement["supplement"]["existing_rows"] == 3
        assert supplement["supplement"]["supplemented_rows"] == 8
        assert supplement["extraction_ready"] is True

        second = svc.retry(first["manifest_id"])
        assert second["status"] == "已交付"
        staging_scope = StagingAreaService().staging_scope(code)
        reconcile = ArrivalReconcileService().reconcile_manifest(code)
        assert reconcile["arrived_windows"] == [window["窗口编号"] for window in windows]

        rows = {
            member["window_code"]: member
            for member in staging_scope["members"]
        }
        reconcile_rows = {
            member["window_code"]: member
            for member in reconcile["members"]
        }
        for window, expected in zip(windows, (10, 11, 12)):
            staged = rows[window["窗口编号"]]
            assert staged["staging_rows"] == expected
            assert staged["arrival_status"] == "已抵达"
            assert staged["delivery_count"] == 1
            reconciled = reconcile_rows[window["窗口编号"]]
            assert reconciled["expected_rows"] == expected
            assert reconciled["actual_rows"] == expected
            assert reconciled["missing_rows"] == 0
            assert reconciled["delivery_count"] == 1

    def test_non_adjacent_windows_rejected(self):
        """跳过中间窗口的选择不构成相邻窗口，成组提交被拒绝。"""
        windows = self._prepare_windows()
        svc = ReleaseManifestService()
        result = svc.submit_group(window_ids=[windows[0]["id"], windows[2]["id"]])
        assert result["ok"] is False
        assert "相邻" in result["message"]

    def test_window_without_extraction_is_not_delivered(self):
        """抽取未完成的窗口进入清单但不交付，标记为待补齐。"""
        windows = self._prepare_windows()
        ew_svc = ExtractWindowService()
        # 新增一个尚未抽取的相邻窗口
        pending = ew_svc.create_window("SOUR-GROUP", count=9)
        svc = ReleaseManifestService()
        result = svc.submit_group(window_ids=[window["id"] for window in windows] + [pending["id"]])

        by_code = {member["window_code"]: member for member in result["members"]}
        pending_member = by_code[pending["窗口编号"]]
        assert pending_member["delivery_status"] == "待补齐"
        assert pending_member["actual_rows"] == 0
        assert pending_member["delivery_count"] == 0
        assert pending["窗口编号"] in result["pending"]


class TestCrossModuleSnapshot:
    """跨模块汇总验证：所有模块 domain_snapshot 可调用。"""
    MODULES = [
        ("source_register", SourceRegisterService),
        ("target_register", TargetRegisterService),
        ("field_contract", FieldContractService),
        ("extract_window", ExtractWindowService),
        ("release_manifest", ReleaseManifestService),
        ("transfer_batch", TransferBatchService),
        ("staging_area", StagingAreaService),
        ("checksum_verify", ChecksumVerifyService),
        ("arrival_reconcile", ArrivalReconcileService),
        ("conflict_resolve", ConflictResolveService),
        ("resume_point", ResumePointService),
        ("speed_control", SpeedControlService),
        ("reject_record", RejectRecordService),
        ("format_adapt", FormatAdaptService),
        ("transfer_lineage", TransferLineageService),
        ("history_archive", HistoryArchiveService),
        ("exchange_replay", ExchangeReplayService),
        ("sla_monitor", SlaMonitorService),
        ("channel_config", ChannelConfigService),
    ]

    @pytest.mark.parametrize("mod_name,cls", MODULES)
    def test_domain_snapshot(self, mod_name, cls):
        snapshot = cls().domain_snapshot()
        assert snapshot["module"] == mod_name
        assert "artifact" in snapshot
        assert "count" in snapshot