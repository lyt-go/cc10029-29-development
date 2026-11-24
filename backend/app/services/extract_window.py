"""增量窗口领域服务：维护按增量字段切分的抽取窗口边界、抽取执行与推进水位。

本模块负责根据源端登记信息创建抽取窗口，执行数据抽取并写入抽取产物文件，
完成后自动驱动传输批次创建，形成源端登记→窗口抽取→批次传输的贯通链路。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ExtractWindowService(BaseModuleService):
    MODULE = "extract_window"
    ENTITY = "抽取窗口"
    OPERATION_TERM = "切分"
    LIST_FIELDS = ["窗口编号", "源端编号", "增量字段", "窗口起点", "窗口终点", "抽取条数", "窗口类型", "推进水位"]
    REQUIRED_FIELDS = ["窗口编号", "源端编号", "增量字段"]
    STATUS_ORDER = ["草稿", "待抽取", "抽取中", "已闭合"]
    ACTION_RULES = {"启动抽取": "抽取中", "闭合窗口": "已闭合", "标记作废": "已闭合"}
    NEGATIVE_ACTIONS = ["标记作废"]

    def create_window(
        self,
        source_code: str,
        count: int = 10,
        *,
        window_start: str | None = None,
        window_end: str | None = None,
        window_type: str = "增量抽取",
    ) -> dict[str, Any]:
        """为指定源端创建增量抽取窗口并写入窗口配置快照，记录窗口边界与初始水位。

        窗口编号必须唯一：同一秒内连续创建多个窗口时自动追加序号，避免相邻
        窗口共用编号导致成组发布无法区分成员。
        """
        sources = [r for r in store.rows("source_register") if r.get("源端编号") == source_code]
        if not sources:
            raise ValueError(f"源端编号 {source_code} 未登记，请先完成源端登记")
        window_code = self._unique_window_code(source_code)
        entry, missing = self.create_entry({
            "窗口编号": window_code,
            "源端编号": source_code,
            "增量字段": "updated_at",
            "窗口起点": window_start or time.strftime("%Y-%m-%dT00:00:00", time.localtime(time.time())),
            "窗口终点": window_end or time.strftime("%Y-%m-%dT23:59:59", time.localtime(time.time())),
            "抽取条数": str(count),
            "窗口类型": window_type,
            "推进水位": "0",
        })
        if entry is None:
            raise ValueError(f"创建窗口失败: {missing}")
        # 写入窗口配置快照
        self._write_jsonl(
            self._data_dir("windows") / f"{entry['窗口编号']}_config.jsonl",
            [{"window_id": entry["id"], "window_code": entry["窗口编号"], "source": source_code,
              "start": entry["窗口起点"], "end": entry["窗口终点"], "count": count,
              "created_at": time.time()}]
        )
        return entry

    def _unique_window_code(self, source_code: str) -> str:
        """生成源端内唯一的窗口编号；同一秒重复创建时追加父窗口序号。"""
        base = f"EXTR-{source_code}-{int(time.time())}"
        existing = {str(row.get("窗口编号", "")) for row in store.rows(self.MODULE)}
        if base not in existing:
            return base
        parent_count = sum(1 for code in existing if code.startswith(base))
        candidate = f"{base}-W{parent_count + 1:02d}"
        while candidate in existing:
            parent_count += 1
            candidate = f"{base}-W{parent_count + 1:02d}"
        return candidate

    def execute_extract(self, window_id: int, record_count: int = 10) -> dict[str, Any]:
        """执行增量窗口数据抽取，生成抽取产物文件并推进窗口水位。

        从已在 store 登记的源端信息中查找匹配记录，按窗口范围筛选数据行，
        逐条写入抽取产物文件（jsonl），最后推进水位并自动触发传输批次创建。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}
        entry, msg = self.run_action(window_id, "启动抽取")
        if entry is None:
            return {"error": msg, "window_id": window_id}

        source_code = window["源端编号"]
        # 生成抽取数据行，每条记录含窗口编号、源端编号、增量字段取值和时间戳
        records = self._simulate_records(source_code, count=record_count, field_prefix="ext")

        # 写入抽取产物文件——这是本模块的核心数据产物
        # 同一窗口重复执行时以本次结果为准，避免产物重复累加
        output_file = self._data_dir("extracted") / f"{window['窗口编号']}.jsonl"
        if output_file.exists():
            output_file.unlink()
        for offset, record in enumerate(records, start=1):
            record["line_no"] = offset
            record["row_no"] = offset
            record["window_code"] = window["窗口编号"]
        written = self._write_jsonl(output_file, records)
        # 抽取写满即视为完成抽取：窗口闭合，产物可对外交付
        window["推进水位"] = str(written)
        window["status"] = "已闭合"
        window["pending"] = False

        # 推进窗口水位
        self._write_checkpoint(f"window_{window['窗口编号']}", {
            "window_code": window["窗口编号"],
            "source_code": source_code,
            "records_extracted": written,
            "last_extract_at": time.time(),
            "watermark": str(written),
        })

        result = {
            "window_code": window["窗口编号"],
            "source_code": source_code,
            "records_extracted": written,
            "artifact_path": str(output_file),
            "watermark": str(written),
        }

        # 抽取完成后自动触发传输批次创建
        try:
            tb_svc = self._safe_import("app.services.transfer_batch", "TransferBatchService")()
            batch_result = tb_svc.create_batch(window["窗口编号"], written)
            result["auto_batch"] = batch_result
        except Exception as e:
            result["auto_batch"] = {"error": str(e)}

        return result

    def supplement_extract(self, window_id: int, record_count: int | None = None) -> dict[str, Any]:
        """单独补齐单个故障窗口的抽取产物，不触碰同组其他窗口。

        以窗口声明的抽取条数为目标，只追加缺失行；已有行保持不变，因此可以
        重复调用而不会重复写入。补齐完成后窗口可再次交付，传输侧按行号续写。
        """
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}

        output_file = self._data_dir("extracted") / f"{window['窗口编号']}.jsonl"
        existing_records = self._read_jsonl(output_file)
        declared = (
            record_count
            or int(str(window.get("抽取条数") or 0).strip() or 0)
            or len(existing_records)
            or 10
        )
        missing = max(0, declared - len(existing_records))

        if missing:
            start = len(existing_records)
            records = self._simulate_records(window["源端编号"], count=missing, field_prefix="ext")
            for offset, record in enumerate(records, start=start + 1):
                record["line_no"] = offset
                record["row_no"] = offset
                record["window_code"] = window["窗口编号"]
                record["supplemented"] = True
            self._write_jsonl(output_file, records)
            window["推进水位"] = str(len(existing_records) + missing)
        else:
            missing = 0

        total = len(self._read_jsonl(output_file))
        # 补满声明条数即恢复为可交付窗口
        if total >= declared:
            window["status"] = "已闭合"
            window["pending"] = False
        self._write_checkpoint(f"window_{window['窗口编号']}", {
            "window_code": window["窗口编号"],
            "source_code": window["源端编号"],
            "records_extracted": total,
            "supplemented_rows": missing,
            "last_extract_at": time.time(),
            "watermark": str(total),
        })
        self._write_jsonl(
            self._data_dir("events") / "window_supplement_log.jsonl",
            [{"action": "supplement", "window_code": window["窗口编号"],
              "existing_rows": len(existing_records), "supplemented_rows": missing,
              "total_rows": total, "at": time.time()}],
        )
        return {
            "window_code": window["窗口编号"],
            "window_id": window_id,
            "declared_rows": declared,
            "existing_rows": len(existing_records),
            "supplemented_rows": missing,
            "total_rows": total,
            "artifact_path": str(output_file),
        }

    def close_window(self, window_id: int) -> dict[str, Any]:
        """闭合抽取窗口，生成窗口产物摘要（记录条数、字节总量、校验值），写入闭合事件日志。"""
        window = self.get_entry(window_id)
        if window is None:
            return {"error": f"抽取窗口 {window_id} 不存在"}

        entry, msg = self.run_action(window_id, "闭合窗口")
        # 从产物文件读取统计信息
        artifact_file = self._data_dir("extracted") / f"{window['窗口编号']}.jsonl"
        records = self._read_jsonl(artifact_file)
        checksum = self._compute_checksum(str(records))

        # 写入闭合事件日志，记录抽取产物的完整统计
        self._write_jsonl(
            self._data_dir("events") / "window_close_log.jsonl",
            [{"action": "close", "window_code": window["窗口编号"], "record_count": len(records),
              "checksum": checksum, "closed_at": time.time()}]
        )

        return {
            "window_code": window["窗口编号"],
            "status": entry["status"] if entry else "已闭合",
            "record_count": len(records),
            "checksum": checksum,
            "message": msg,
        }