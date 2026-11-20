"""接入质量校验领域服务：字段级校验、完整性检查、阈值告警与阻断策略。

提供转换后数据的质量校验能力，包括必填字段检查、类型校验、
数值范围校验和完整性评分。
跨模块关联：data_transform（转换后校验）、batch_ingest（批量质量检查）。
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class DataQualityRuleService(BaseModuleService):
    MODULE = "data_quality_rule"
    ENTITY = "质量规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "数据来源", "校验字段", "校验表达式", "阈值下界", "阈值上界", "阻断策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "数据来源", "校验字段"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def __init__(self) -> None:
        super().__init__()
        self._validation_history: list[dict[str, Any]] = []

    RELEASE_MODULE = "quality_release_list"

    @staticmethod
    def build_source_row_key(source_module: str, page: int, identity: Any) -> str:
        """按来源行身份生成稳定键，跨批次、跨续做可关联同一条来源行。"""
        return f"{source_module}:p{page}:src{identity}"

    def build_release_list(
        self,
        task_id: int,
        batch_id: str,
        page: int,
        records: list[dict[str, Any]],
        source_module: str = "batch_ingest",
        identity_field: str = "id",
    ) -> dict[str, Any]:
        """对同页转换数据做批量校验，产出可直接使用的质检放行清单。

        清单按 (任务ID, 批次号, 页码) 幂等：同页重复调用直接复用已持久化
        的清单，保证两轮批次重叠时同一来源行只有一份放行结论。
        每条记录同时给出可落湖结论（放行/阻断）与阻断原因，交给接入流程。

        Returns:
            { ok, 清单编号, 任务ID, 批次号, 页码, total, 放行数, 阻断数, items }
        """
        existing = self._find_release_list(task_id, batch_id, page)
        if existing is not None:
            items = existing.get("items", [])
            return {
                "ok": True,
                "清单编号": existing.get("清单编号"),
                "任务ID": task_id,
                "批次号": batch_id,
                "页码": page,
                "reused": True,
                "total": len(items),
                "放行数": sum(1 for it in items if it["放行结论"] == "放行"),
                "阻断数": sum(1 for it in items if it["放行结论"] == "阻断"),
                "items": items,
            }

        items: list[dict[str, Any]] = []
        released = 0
        blocked = 0
        for record in records:
            identity = record.get(identity_field)
            source_row_key = self.build_source_row_key(source_module, page, identity)
            verdict = self.validate_record(record, source_module=source_module)
            is_blocked = bool(verdict.get("blocked")) or not verdict.get("passed", True)
            block_reasons: list[str] = []
            if is_blocked:
                for detail in verdict.get("details", []):
                    if not detail.get("passed", True):
                        block_reasons.append(
                            f"{detail.get('rule_no')}/{detail.get('field')}: "
                            f"{self._first_failure_message(detail)}"
                        )
                if not block_reasons:
                    block_reasons.append("质量校验未通过")
            conclusion = "阻断" if is_blocked else "放行"
            if is_blocked:
                blocked += 1
            else:
                released += 1
            items.append({
                "来源行身份": source_row_key,
                "来源行标识": identity,
                "放行结论": conclusion,
                "阻断原因": "；".join(block_reasons),
                "校验分数": verdict.get("score", 100),
                "阻断策略命中": bool(verdict.get("blocked")),
                "行数据快照": dict(record),
                "生成时间": datetime.now(timezone.utc).isoformat(),
            })

        list_no = f"QRL-{task_id}-{page}-{uuid.uuid4().hex[:8]}"
        persisted = json_store.insert(self.RELEASE_MODULE, {
            "清单编号": list_no,
            "任务ID": task_id,
            "批次号": batch_id,
            "页码": page,
            "来源模块": source_module,
            "total": len(items),
            "放行数": released,
            "阻断数": blocked,
            "items": items,
            "生成时间": datetime.now(timezone.utc).isoformat(),
        })

        return {
            "ok": True,
            "清单编号": list_no,
            "清单记录ID": persisted.get("id"),
            "任务ID": task_id,
            "批次号": batch_id,
            "页码": page,
            "reused": False,
            "total": len(items),
            "放行数": released,
            "阻断数": blocked,
            "items": items,
        }

    @staticmethod
    def _first_failure_message(detail: dict[str, Any]) -> str:
        for key in ("expr_result", "range_result"):
            result = detail.get(key) or {}
            if not result.get("passed", True):
                return str(result.get("message", "校验失败"))
        return "校验失败"

    def _find_release_list(self, task_id: int, batch_id: str, page: int) -> dict[str, Any] | None:
        for row in json_store.get_all(self.RELEASE_MODULE):
            if (row.get("任务ID") == task_id
                    and row.get("批次号") == batch_id
                    and row.get("页码") == page):
                return row
        return None

    def get_release_lists(
        self,
        task_id: int | None = None,
        batch_id: str | None = None,
        page: int | None = None,
    ) -> list[dict[str, Any]]:
        """查询质检放行清单，可按任务、批次、页码过滤。"""
        rows = json_store.get_all(self.RELEASE_MODULE)
        result = []
        for row in rows:
            if task_id is not None and row.get("任务ID") != task_id:
                continue
            if batch_id is not None and row.get("批次号") != batch_id:
                continue
            if page is not None and row.get("页码") != page:
                continue
            result.append(row)
        return result

    def get_release_decision(self, source_row_key: str, task_id: int | None = None) -> dict[str, Any] | None:
        """按来源行身份查最近一次放行结论；阻断行可据此确认始终拿不到落湖回执。"""
        latest: dict[str, Any] | None = None
        for row in json_store.get_all(self.RELEASE_MODULE):
            if task_id is not None and row.get("任务ID") != task_id:
                continue
            for item in row.get("items", []):
                if item.get("来源行身份") == source_row_key:
                    latest = item
        return latest

    def validate_record(self, record: dict[str, Any], source_module: str = "") -> dict[str, Any]:
        """对单条记录执行质量校验。

        校验流程：
        1. 加载与该来源相关的已生效规则
        2. 逐条规则执行校验表达式
        3. 检查数值范围（阈值上下界）
        4. 根据阻断策略决定是否拒绝

        Returns:
            { passed: bool, checks: int, failed: int, details: list, score: int }
        """
        rules = json_store.get_all(self.MODULE)
        matching = [
            r for r in rules
            if r.get("status") == "已生效"
            and (str(r.get("数据来源", "")) in (source_module, "", "ALL", "*")
                 or source_module == ""
                 or not source_module)
        ]

        if not matching:
            return {"passed": True, "checks": 0, "failed": 0, "details": [], "score": 100}

        results = []
        total_passed = 0
        total_failed = 0

        for rule in matching:
            field = str(rule.get("校验字段", ""))
            expression = str(rule.get("校验表达式", ""))
            block_policy = str(rule.get("阻断策略", "警告"))
            try:
                lower = float(rule.get("阈值下界", "0") or "0")
                upper = float(rule.get("阈值上界", "0") or "0")
            except ValueError:
                lower = 0
                upper = 0

            check_result = self._execute_check(record, field, expression)
            range_check = self._check_range(record, field, lower, upper)

            passed = check_result.get("passed", True) and range_check.get("passed", True)
            detail = {
                "rule_no": rule.get("规则编号"),
                "field": field,
                "expression": expression,
                "passed": passed,
                "expr_result": check_result,
                "range_result": range_check,
                "block_policy": block_policy,
            }
            results.append(detail)

            if passed:
                total_passed += 1
            else:
                total_failed += 1
                if block_policy == "阻断":
                    self._validation_history.append({
                        "rule": rule.get("规则编号"),
                        "field": field,
                        "outcome": "blocked",
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    })
                    return {
                        "passed": False,
                        "checks": len(matching),
                        "failed": total_failed,
                        "passed_checks": total_passed,
                        "details": results,
                        "score": max(0, 100 - total_failed * 10),
                        "blocked": True,
                    }

        self._validation_history.append({
            "record_preview": str(record)[:100],
            "checks": len(matching),
            "passed": total_passed,
            "failed": total_failed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        return {
            "passed": total_failed == 0,
            "checks": len(matching),
            "failed": total_failed,
            "passed_checks": total_passed,
            "details": results,
            "score": max(0, 100 - total_failed * 10),
        }

    def _execute_check(self, record: dict[str, Any], field: str, expression: str) -> dict[str, Any]:
        """执行校验表达式。

        支持的表达式操作：
        - "not_empty": 非空检查
        - "is_numeric": 是否为数字
        - "pattern:xxx": 正则匹配
        - "in:val1,val2": 值域检查
        - "len_gt:N": 长度大于N
        """
        value = record.get(field)
        field_present = field in record

        if expression == "not_empty":
            if not field_present or value is None or str(value).strip() == "":
                return {"passed": False, "message": f"字段 {field} 为空"}
            return {"passed": True, "message": "通过"}

        if expression == "is_numeric":
            try:
                float(str(value))
                return {"passed": True, "message": "通过"}
            except (ValueError, TypeError):
                return {"passed": False, "message": f"字段 {field} 不是数字"}

        if expression.startswith("pattern:"):
            pattern = expression.split(":", 1)[1]
            if not re.match(pattern, str(value)):
                return {"passed": False, "message": f"字段 {field} 不匹配模式 {pattern}"}
            return {"passed": True, "message": "通过"}

        if expression.startswith("in:"):
            allowed = expression.split(":", 1)[1].split(",")
            if str(value) not in allowed:
                return {"passed": False, "message": f"字段 {field} 不在允许值 {allowed} 中"}
            return {"passed": True, "message": "通过"}

        if expression.startswith("len_gt:"):
            min_len = int(expression.split(":", 1)[1])
            if len(str(value or "")) < min_len:
                return {"passed": False, "message": f"字段 {field} 长度小于 {min_len}"}
            return {"passed": True, "message": "通过"}

        return {"passed": True, "message": "未识别表达式，默认通过"}

    def _check_range(self, record: dict[str, Any], field: str, lower: float, upper: float) -> dict[str, Any]:
        """检查字段值是否在阈值范围内。"""
        if lower == 0 and upper == 0:
            return {"passed": True, "message": "无范围限制"}

        value = record.get(field)
        try:
            num_value = float(str(value))
        except (ValueError, TypeError):
            return {"passed": True, "message": "非数值字段跳过范围检查"}

        if lower > 0 and num_value < lower:
            return {"passed": False, "message": f"值 {num_value} 低于阈值 {lower}"}
        if upper > 0 and num_value > upper:
            return {"passed": False, "message": f"值 {num_value} 超过阈值 {upper}"}

        return {"passed": True, "message": f"值 {num_value} 在范围 [{lower}, {upper}] 内"}

    def validate_batch(self, records: list[dict[str, Any]], source_module: str = "") -> dict[str, Any]:
        """批量质量校验。"""
        results = []
        passed_count = 0
        failed_count = 0
        blocked_count = 0

        for record in records:
            r = self.validate_record(record, source_module)
            if r.get("blocked"):
                blocked_count += 1
                failed_count += 1
            elif r["passed"]:
                passed_count += 1
            else:
                failed_count += 1
            results.append(r)

        avg_score = sum(r.get("score", 0) for r in results) / max(len(results), 1)

        return {
            "ok": blocked_count == 0,
            "total": len(records),
            "passed": passed_count,
            "failed": failed_count,
            "blocked": blocked_count,
            "average_score": round(avg_score, 2),
            "results": results,
        }

    def get_quality_report(self) -> dict[str, Any]:
        """获取质量校验报告。"""
        history = self._validation_history[-100:]
        rules = json_store.get_all(self.MODULE)
        active_rules = sum(1 for r in rules if r.get("status") == "已生效")

        return {
            "active_rules": active_rules,
            "total_checks": len(history),
            "recent_failures": sum(1 for h in history if h.get("failed", 0) > 0),
            "recent_blocks": sum(1 for h in history if h.get("blocked")),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出接入质量校验的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "接入质量校验数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_validations": len(self._validation_history),
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "release_lists": len(json_store.get_all(self.RELEASE_MODULE)),
            "released_rows": sum(
                int(r.get("放行数", 0)) for r in json_store.get_all(self.RELEASE_MODULE)
            ),
            "blocked_rows": sum(
                int(r.get("阻断数", 0)) for r in json_store.get_all(self.RELEASE_MODULE)
            ),
            "operation": self.OPERATION_TERM,
        }