"""批量接入调度领域服务：定时批量拉取、分页控制、断点续传与任务编排。

支持基于Cron表达式的定时拉取、分页遍历、并行批次调度和
断点续传能力。拉取的数据经过转换后先进入质检放行清单，仅放行行
写入数据湖，并按来源行身份生成接入结算清单与落湖回执。
跨模块关联：api_endpoint（拉取源端点）、data_transform（数据转换）、
data_quality_rule（质检放行清单）、data_lake_sink（落湖回执）、
incre_cursor（增量标记）。
"""
from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService

# 接入结算清单（按任务-批次-页持久化）
SETTLEMENT_MODULE = "ingest_settlement"
# 断点续传检查点（按任务持久化，中断后续做的权威依据）
CHECKPOINT_MODULE = "batch_ingest_checkpoint"


class BatchIngestService(BaseModuleService):
    MODULE = "batch_ingest"
    ENTITY = "接入任务"
    OPERATION_TERM = "入库"
    LIST_FIELDS = ["任务编号", "关联端点", "Cron表达式", "分页策略", "并行批次", "断点续传", "失败动作", "任务版本"]
    REQUIRED_FIELDS = ["任务编号", "关联端点", "Cron表达式"]
    STATUS_ORDER = ["草稿", "待校验", "运行中", "已暂停"]
    ACTION_RULES = {"提交校验": "待校验", "启动任务": "运行中", "暂停任务": "已暂停"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        # 内存镜像仅作兼容保留，权威状态在 json_store 持久化分区
        self._checkpoints: dict[int, int] = {}
        self._execution_history: list[dict[str, Any]] = []

    # ------------------------------------------------------------------
    # 断点检查点（持久化）
    # ------------------------------------------------------------------
    def _load_checkpoint(self, task_id: int) -> dict[str, Any] | None:
        for row in json_store.get_all(CHECKPOINT_MODULE):
            if row.get("任务ID") == task_id:
                return row
        return None

    def _save_checkpoint(self, task_id: int, batch_id: str, page: int,
                         page_size: int, interrupted: bool) -> dict[str, Any]:
        existing = self._load_checkpoint(task_id)
        payload = {
            "任务ID": task_id,
            "批次号": batch_id,
            "当前页": page,
            "分页大小": page_size,
            "中断标记": interrupted,
            "更新时间": datetime.now(timezone.utc).isoformat(),
        }
        if existing is None:
            saved = json_store.insert(CHECKPOINT_MODULE, payload)
        else:
            saved = json_store.update(CHECKPOINT_MODULE, existing["id"], payload) or payload
        self._checkpoints[task_id] = page
        return saved

    def _clear_checkpoint(self, task_id: int) -> None:
        existing = self._load_checkpoint(task_id)
        if existing is not None:
            json_store.delete(CHECKPOINT_MODULE, existing["id"])
        self._checkpoints.pop(task_id, None)

    # ------------------------------------------------------------------
    # 接入结算清单（持久化）
    # ------------------------------------------------------------------
    def _find_settlement(self, task_id: int, batch_id: str, page: int) -> dict[str, Any] | None:
        for row in json_store.get_all(SETTLEMENT_MODULE):
            if (row.get("任务ID") == task_id
                    and row.get("批次号") == batch_id
                    and row.get("页码") == page):
                return row
        return None

    def _upsert_settlement(self, settlement: dict[str, Any]) -> dict[str, Any]:
        existing = self._find_settlement(
            int(settlement["任务ID"]), str(settlement["批次号"]), int(settlement["页码"])
        )
        summary = self._summarize_settlement(settlement)
        settlement.update(summary)
        if existing is None:
            return json_store.insert(SETTLEMENT_MODULE, settlement)
        return json_store.update(SETTLEMENT_MODULE, existing["id"], settlement) or settlement

    @staticmethod
    def _summarize_settlement(settlement: dict[str, Any]) -> dict[str, Any]:
        rows = settlement.get("rows", [])
        released = sum(1 for r in rows if r.get("结论") == "放行")
        return {
            "放行数": released,
            "阻断数": sum(1 for r in rows if r.get("结论") == "阻断"),
            "已处理数": sum(1 for r in rows if r.get("回执")),
            "新落湖数": sum(1 for r in rows if r.get("状态") == "已落湖"),
            "重复跳过数": sum(1 for r in rows if r.get("状态") == "重复跳过"),
            "待处置数": sum(1 for r in rows if r.get("状态") == "待处置"),
            "页关闭": all(r.get("状态") != "待处置" for r in rows) and bool(rows),
        }

    def get_settlement_lists(self, task_id: int | None = None,
                             batch_id: str | None = None) -> list[dict[str, Any]]:
        """查询接入结算清单，已处理数来自数据落湖的实际回执。"""
        result = []
        for row in json_store.get_all(SETTLEMENT_MODULE):
            if task_id is not None and row.get("任务ID") != task_id:
                continue
            if batch_id is not None and row.get("批次号") != batch_id:
                continue
            result.append(row)
        return result

    # ------------------------------------------------------------------
    # 主流程
    # ------------------------------------------------------------------
    def execute_batch(self, task_id: int, max_pages: int = 5, page_size: int = 20,
                      *, injected_pages: dict[int, list[dict[str, Any]]] | None = None,
                      interrupt_after: int | None = None) -> dict[str, Any]:
        """执行批量拉取任务。

        完整流程：
        1. 验证任务状态，读取持久化断点（中断续做复用原批次号）
        2. 逐页拉取模拟数据并做同页转换
        3. 跨模块消费质检放行清单：可落湖记录与阻断原因一次性产出
        4. 仅放行行按来源行身份写入数据湖，身份注册表保证不重复落湖
        5. 按页生成接入结算清单，已处理数取自落湖实际回执
        6. 中断时持久化待处置行，续做时依据来源行身份关联清单与回执

        Args:
            injected_pages: 测试用确定性分页数据 {页码: [记录]}
            interrupt_after: 新写入达到该条数后模拟写入中断（None 表示不中断）

        Returns:
            { ok, pages_processed, records_fetched, records_sunk, ... }
        """
        task = json_store.get_by_id(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": f"任务 {task_id} 不存在", "pages_processed": 0}

        if task.get("status") != "运行中":
            return {"ok": False, "message": f"任务状态{task['status']}，无法执行", "pages_processed": 0}

        endpoint_id = int(task.get("关联端点", "0") or "0")
        enable_checkpoint = str(task.get("断点续传", "否")) in ("是", "true", "True", "Y")
        on_failure = str(task.get("失败动作", "记录并继续"))

        # 断点续做：优先使用持久化检查点中的原批次号与中断页
        checkpoint = self._load_checkpoint(task_id) if enable_checkpoint else None
        if checkpoint is not None:
            batch_id = str(checkpoint["批次号"])
            start_page = int(checkpoint["当前页"])
            page_size = int(checkpoint.get("分页大小", page_size) or page_size)
        else:
            batch_id = f"BATCH-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
            start_page = 1

        total_fetched = 0
        total_sunk = 0
        total_duplicates = 0
        total_blocked = 0
        pages_processed = 0
        errors: list[str] = []
        interrupted = False

        from app.services.data_quality_rule import DataQualityRuleService
        from app.services.data_lake_sink import DataLakeSinkService
        quality_svc = DataQualityRuleService()
        sink_svc = DataLakeSinkService()

        for page in range(start_page, start_page + max_pages):
            pages_processed += 1

            # 模拟拉取数据（允许注入确定性数据，便于验证放行边界）
            if injected_pages is not None and page in injected_pages:
                mock_data = injected_pages[page]
            else:
                mock_data = self._mock_fetch_page(endpoint_id, page, page_size)
            if not mock_data:
                break

            total_fetched += len(mock_data)

            # 数据转换（跨模块：data_transform）
            transformed = self._transform_batch(endpoint_id, mock_data)

            # 质检放行清单（跨模块：data_quality_rule），同页幂等可复用
            release = quality_svc.build_release_list(
                task_id=task_id,
                batch_id=batch_id,
                page=page,
                records=transformed,
                source_module=self.MODULE,
            )

            # 本页结算清单：中断续做时加载已持久化的行级状态
            settlement = self._find_settlement(task_id, batch_id, page)
            if settlement is None:
                settlement = {
                    "任务ID": task_id,
                    "批次号": batch_id,
                    "页码": page,
                    "清单编号": release.get("清单编号"),
                    "rows": [],
                    "创建时间": datetime.now(timezone.utc).isoformat(),
                }
            known_keys = {r["来源行身份"] for r in settlement["rows"]}

            for item in release["items"]:
                source_row_key = item["来源行身份"]
                if source_row_key in known_keys:
                    continue  # 续做：保留已落湖/待处置行的既有状态与回执
                row_state: dict[str, Any] = {
                    "来源行身份": source_row_key,
                    "来源行标识": item.get("来源行标识"),
                    "结论": item["放行结论"],
                    "状态": "待处置",
                    "阻断原因": item.get("阻断原因", ""),
                    "回执": None,
                }
                if item["放行结论"] == "阻断":
                    # 阻断行不进入落湖流程，始终查不到正式落湖记录
                    row_state["状态"] = "阻断"
                settlement["rows"].append(row_state)

            # 仅放行且待处置的行进入落湖，按来源行身份幂等
            for row_state in settlement["rows"]:
                if row_state["状态"] != "待处置" or row_state["结论"] != "放行":
                    continue
                source_row_key = row_state["来源行身份"]
                payload = self._payload_for_row(release, source_row_key)
                try:
                    result = sink_svc.write_event(
                        f"{batch_id}:{uuid.uuid4().hex[:8]}",
                        "batch_ingest",
                        payload,
                        source_channel_id=endpoint_id,
                        source_row_key=source_row_key,
                    )
                except Exception as e:  # pragma: no cover - 防御性分支
                    errors.append(str(e))
                    if on_failure == "中断任务":
                        self._persist_interruption(
                            task_id, batch_id, page, page_size, settlement
                        )
                        return self._interrupt_response(
                            task_id, batch_id, pages_processed, total_fetched,
                            total_sunk, total_duplicates, total_blocked, errors,
                        )
                    continue

                if not result.get("ok"):
                    errors.append(f"落湖失败: {result.get('message')}")
                    if on_failure == "中断任务":
                        self._persist_interruption(
                            task_id, batch_id, page, page_size, settlement
                        )
                        return self._interrupt_response(
                            task_id, batch_id, pages_processed, total_fetched,
                            total_sunk, total_duplicates, total_blocked, errors,
                        )
                    continue

                receipt = {
                    "event_id": result.get("event_id"),
                    "partition": result.get("partition"),
                    "file_path": result.get("file_path"),
                    "record_hash": result.get("record_hash"),
                    "sunk_at": datetime.now(timezone.utc).isoformat(),
                }
                row_state["回执"] = receipt
                if result.get("duplicate"):
                    # 两轮批次重叠：身份注册表命中，只保留一份合格行
                    row_state["状态"] = "重复跳过"
                    total_duplicates += 1
                else:
                    row_state["状态"] = "已落湖"
                    total_sunk += 1

                self._upsert_settlement(settlement)

                # 模拟批量写入停在一半：先持久化待处置行再返回
                if interrupt_after is not None and total_sunk >= interrupt_after:
                    interrupted = True
                    self._persist_interruption(
                        task_id, batch_id, page, page_size, settlement
                    )
                    total_blocked = sum(1 for r in settlement["rows"] if r["结论"] == "阻断")
                    return self._interrupt_response(
                        task_id, batch_id, pages_processed, total_fetched,
                        total_sunk, total_duplicates, total_blocked, errors,
                    )

            # 页关闭：本页放行/阻断全部处置完毕，结算清单持久化，断点前移
            self._upsert_settlement(settlement)
            total_blocked += sum(1 for r in settlement["rows"] if r["结论"] == "阻断")
            if enable_checkpoint:
                self._save_checkpoint(task_id, batch_id, page + 1, page_size, interrupted=False)

            time.sleep(0.01)

        # 更新增量水位（跨模块：incre_cursor）
        self._update_incre_cursor(task_id, endpoint_id, total_fetched)

        json_store.update(self.MODULE, task_id, {
            "last_batch_id": batch_id,
            "last_executed_at": datetime.now(timezone.utc).isoformat(),
            "total_processed": int(task.get("total_processed", 0)) + total_sunk,
        })

        self._execution_history.append({
            "task_id": task_id,
            "batch_id": batch_id,
            "pages": pages_processed,
            "fetched": total_fetched,
            "sunk": total_sunk,
            "duplicates": total_duplicates,
            "blocked": total_blocked,
            "errors": len(errors),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        if enable_checkpoint and not interrupted:
            # 全部页处置完毕才清理断点，保证中断续做闭环
            pending = [
                s for s in self.get_settlement_lists(task_id, batch_id)
                if not s.get("页关闭")
            ]
            if not pending:
                self._clear_checkpoint(task_id)

        return {
            "ok": len(errors) == 0 or total_sunk > 0 or total_duplicates > 0,
            "interrupted": False,
            "message": (
                f"批量接入完成: {pages_processed} 页, {total_fetched} 条, "
                f"落湖 {total_sunk} 条, 重复跳过 {total_duplicates} 条, 阻断 {total_blocked} 条"
            ),
            "batch_id": batch_id,
            "pages_processed": pages_processed,
            "records_fetched": total_fetched,
            "records_sunk": total_sunk,
            "duplicates_skipped": total_duplicates,
            "records_blocked": total_blocked,
            "errors": errors,
        }

    @staticmethod
    def _payload_for_row(release: dict[str, Any], source_row_key: str) -> dict[str, Any]:
        for item in release["items"]:
            if item["来源行身份"] == source_row_key:
                payload = dict(item.get("行数据快照", {}))
                payload["_source_row_key"] = source_row_key
                return payload
        return {"_source_row_key": source_row_key}

    def _persist_interruption(self, task_id: int, batch_id: str, page: int,
                              page_size: int, settlement: dict[str, Any]) -> None:
        self._upsert_settlement(settlement)
        self._save_checkpoint(task_id, batch_id, page, page_size, interrupted=True)

    def _interrupt_response(self, task_id: int, batch_id: str, pages_processed: int,
                            total_fetched: int, total_sunk: int, total_duplicates: int,
                            total_blocked: int, errors: list[str]) -> dict[str, Any]:
        return {
            "ok": False,
            "interrupted": True,
            "message": f"批量写入中断，批次 {batch_id} 已持久化，可续做",
            "batch_id": batch_id,
            "pages_processed": pages_processed,
            "records_fetched": total_fetched,
            "records_sunk": total_sunk,
            "duplicates_skipped": total_duplicates,
            "records_blocked": total_blocked,
            "errors": errors,
        }

    def _mock_fetch_page(self, endpoint_id: int, page: int, size: int) -> list[dict[str, Any]]:
        """模拟从API拉取一页数据。"""
        if page > 10:
            return []
        import random
        return [
            {
                "id": (page - 1) * size + i + 1,
                "name": f"record_{endpoint_id}_{page}_{i}",
                "type": "api_data",
                "value": random.randint(1, 1000),
                "category": random.choice(["A", "B", "C"]),
                "status": random.choice(["active", "pending", "done"]),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            for i in range(min(size, 20))
        ]

    def _transform_batch(self, endpoint_id: int, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """批量转换数据。"""
        try:
            from app.services.data_transform import DataTransformService
            DataTransformService()
            return [
                {"source_endpoint": endpoint_id, "transformed": True, **r}
                for r in records
            ]
        except Exception:
            return [{"source_endpoint": endpoint_id, **r} for r in records]

    def _update_incre_cursor(self, task_id: int, endpoint_id: int, fetched_count: int) -> None:
        """更新增量水位。"""
        try:
            from app.services.incre_cursor import IncreCursorService
            cursor_svc = IncreCursorService()
            cursor_svc.update_position(endpoint_id, fetched_count)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 放行边界与核对
    # ------------------------------------------------------------------
    def get_page_release_boundary(self, task_id: int, batch_id: str, page: int) -> dict[str, Any]:
        """查看某页的放行边界及其在湖里的实际可用情况。

        同一页混有正常行与阻断行时，放行边界决定了湖里已经可用的数据：
        放行行可查到正式落湖记录，阻断行始终查不到。
        """
        from app.services.data_lake_sink import DataLakeSinkService
        from app.services.data_quality_rule import DataQualityRuleService
        quality_svc = DataQualityRuleService()
        sink_svc = DataLakeSinkService()

        release = next(
            (r for r in quality_svc.get_release_lists(task_id, batch_id, page)), None
        )
        settlement = self._find_settlement(task_id, batch_id, page)
        if release is None and settlement is None:
            return {"ok": False, "message": f"任务{task_id}批次{batch_id}第{page}页无放行清单"}

        rows = []
        for item in (release or {}).get("items", []):
            key = item["来源行身份"]
            rows.append({
                "来源行身份": key,
                "放行结论": item["放行结论"],
                "阻断原因": item.get("阻断原因", ""),
                "湖里可用": sink_svc.find_lake_record(key) is not None,
            })
        return {
            "ok": True,
            "任务ID": task_id,
            "批次号": batch_id,
            "页码": page,
            "放行数": sum(1 for r in rows if r["放行结论"] == "放行"),
            "阻断数": sum(1 for r in rows if r["放行结论"] == "阻断"),
            "湖里可用数": sum(1 for r in rows if r["湖里可用"]),
            "页关闭": (settlement or {}).get("页关闭", False),
            "rows": rows,
        }

    def reconcile_source_rows(self, task_id: int) -> dict[str, Any]:
        """核对同一来源行是否被累计写入两次、阻断行是否误入数据湖。

        汇总已写行、待处置行及后续页回执，逐来源行比对：
        - 每个放行身份最多一条正式落湖记录
        - 阻断身份查不到任何正式落湖记录
        - 结算清单已处理数与落湖回执数一致
        """
        from app.services.data_lake_sink import DataLakeSinkService
        sink_svc = DataLakeSinkService()

        settlements = self.get_settlement_lists(task_id=task_id)
        identity_receipts: dict[str, int] = {}
        violations: list[dict[str, Any]] = []
        total_receipts = 0
        total_pending = 0
        total_blocked = 0

        for settlement in settlements:
            for row in settlement["rows"]:
                key = row["来源行身份"]
                receipt = row.get("回执")
                if receipt:
                    total_receipts += 1
                    identity_receipts[key] = identity_receipts.get(key, 0) + 1
                if row.get("状态") == "待处置":
                    total_pending += 1
                if row.get("结论") == "阻断":
                    total_blocked += 1
                    lake_record = sink_svc.find_lake_record(key)
                    if lake_record is not None:
                        violations.append({
                            "来源行身份": key,
                            "问题": "阻断行存在正式落湖记录",
                            "file_path": lake_record.get("event_id"),
                        })

        # 同一来源行在多个批次结算清单里最多只能有一份真实落湖回执，
        # 其余必须是“重复跳过”，累计两次即视为违规
        for key, receipt_count in identity_receipts.items():
            registry = sink_svc.find_sink_receipt(key)
            if registry is None:
                violations.append({
                    "来源行身份": key,
                    "问题": "结算回执在落湖身份注册表中缺失",
                })
            elif receipt_count > 1:
                # 多批次出现同一身份：注册表只有一条，确认无累计两次写入
                lake_hits = 1 if sink_svc.find_lake_record(key) is not None else 0
                if lake_hits != 1:
                    violations.append({
                        "来源行身份": key,
                        "问题": "同一来源行累计两次落湖回执",
                    })

        # 与落湖身份注册表交叉核对：注册表自身不允许同身份重复
        registry_rows = json_store.get_all(DataLakeSinkService.IDENTITY_MODULE)
        registry_counts: dict[str, int] = {}
        for reg in registry_rows:
            key = reg.get("source_row_key", "")
            registry_counts[key] = registry_counts.get(key, 0) + 1
        for key, count in registry_counts.items():
            if count > 1:
                violations.append({"来源行身份": key, "问题": f"落湖身份注册表重复 {count} 条"})

        processed_in_settlement = sum(int(s.get("已处理数", 0)) for s in settlements)
        return {
            "ok": not violations,
            "任务ID": task_id,
            "结算页数": len(settlements),
            "回执总数": total_receipts,
            "结算已处理数": processed_in_settlement,
            "已处理数与回执一致": processed_in_settlement == total_receipts,
            "待处置行数": total_pending,
            "阻断行数": total_blocked,
            "唯一放行身份数": len(registry_counts),
            "违规明细": violations,
        }

    def get_task_progress(self, task_id: int) -> dict[str, Any]:
        """获取任务的执行进度（含持久化断点与结算清单口径）。"""
        task = json_store.get_by_id(self.MODULE, task_id)
        if task is None:
            return {"ok": False, "message": "任务不存在"}

        checkpoint = self._load_checkpoint(task_id)
        history = [h for h in self._execution_history if h["task_id"] == task_id]
        settlements = self.get_settlement_lists(task_id=task_id)

        return {
            "ok": True,
            "task_id": task_id,
            "task_no": task.get("任务编号"),
            "status": task.get("status"),
            "checkpoint_page": (checkpoint or {}).get("当前页", 0),
            "interrupted": bool((checkpoint or {}).get("中断标记")),
            "active_batch_id": (checkpoint or {}).get("批次号"),
            "settlement_pages": len(settlements),
            "settled_processed": sum(int(s.get("已处理数", 0)) for s in settlements),
            "settled_blocked": sum(int(s.get("阻断数", 0)) for s in settlements),
            "pending_rows": sum(int(s.get("待处置数", 0)) for s in settlements),
            "total_processed": task.get("total_processed", 0),
            "execution_count": len(history),
            "last_execution": history[-1] if history else None,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出批量接入调度的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        settlements = json_store.get_all(SETTLEMENT_MODULE)
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "批量接入调度数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_executions": len(self._execution_history),
            "active_tasks": sum(1 for r in rows if r.get("status") == "运行中"),
            "settlement_lists": len(settlements),
            "settled_processed": sum(int(s.get("已处理数", 0)) for s in settlements),
            "settled_blocked": sum(int(s.get("阻断数", 0)) for s in settlements),
            "pending_rows": sum(int(s.get("待处置数", 0)) for s in settlements),
            "checkpoints": len(json_store.get_all(CHECKPOINT_MODULE)),
            "operation": self.OPERATION_TERM,
        }
