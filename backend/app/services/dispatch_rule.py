"""事件分发路由领域服务：事件条件路由、目标系统匹配与多路分发。

根据事件类型和payload内容匹配分发规则，确定目标子系统列表。
支持优先级排序、过滤条件匹配和失败动作配置。
跨模块关联：webhook_channel（事件来源）、data_lake_sink（落湖）、call_trace_log（分发追踪）。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class DispatchRuleService(BaseModuleService):
    MODULE = "dispatch_rule"
    ENTITY = "分发规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则编号", "事件类型", "目标系统", "分发方式", "过滤条件", "优先级", "失败动作", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "事件类型", "目标系统"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def match_targets(self, event_type: str, payload: dict[str, Any]) -> list[dict[str, Any]]:
        """根据事件类型和payload匹配分发规则，返回目标系统列表。

        匹配逻辑：
        1. 查找事件类型匹配且已生效的规则
        2. 对每条规则评估过滤条件
        3. 按优先级排序
        4. 返回目标系统列表

        Args:
            event_type: 事件类型名称
            payload: 事件数据

        Returns:
            匹配的目标系统列表，每项包含目标系统名、分发方式、优先级
        """
        rules = json_store.get_all(self.MODULE)
        matching = [
            r for r in rules
            if r.get("status") == "已生效"
            and str(r.get("事件类型", "")) in (event_type, "通用", "ALL", "*")
        ]

        if not matching:
            return [{
                "目标系统": "default_sink",
                "分发方式": "直连",
                "优先级": "低",
                "规则编号": "DEFAULT",
            }]

        result = []
        for rule in matching:
            # 评估过滤条件
            filter_condition = str(rule.get("过滤条件", ""))
            if filter_condition and not self._evaluate_condition(filter_condition, payload):
                continue

            result.append({
                "目标系统": rule.get("目标系统"),
                "分发方式": rule.get("分发方式", "异步"),
                "优先级": rule.get("优先级", "中"),
                "规则编号": rule.get("规则编号"),
            })

        # 按优先级排序
        priority_order = {"高": 0, "中": 1, "低": 2}
        result.sort(key=lambda x: priority_order.get(str(x["优先级"]), 2))

        return result if result else [{
            "目标系统": "default_sink",
            "分发方式": "直连",
            "优先级": "低",
            "规则编号": "DEFAULT",
        }]

    def _evaluate_condition(self, condition: str, payload: dict[str, Any]) -> bool:
        """评估过滤条件。

        支持的条件格式：
        - JSON: {"field": "status", "op": "eq", "value": "active"}
        - 简单: "field=value" 或多条件用逗号分隔
        """
        try:
            condition_obj = json.loads(condition)
            field = condition_obj.get("field", "")
            op = condition_obj.get("op", "eq")
            expected = condition_obj.get("value", "")

            actual = str(payload.get(field, payload.get(f"data.{field}", "")))
            ops = {
                "eq": lambda a, e: str(a) == str(e),
                "ne": lambda a, e: str(a) != str(e),
                "contains": lambda a, e: str(e) in str(a),
                "gt": lambda a, e: float(a) > float(e),
                "lt": lambda a, e: float(a) < float(e),
            }
            return ops.get(op, lambda a, e: True)(actual, expected)
        except json.JSONDecodeError:
            # 简单格式: "key=value" 或多个用","分隔
            for cond in condition.split(","):
                cond = cond.strip()
                if "=" in cond:
                    key, val = cond.split("=", 1)
                    key = key.strip()
                    val = val.strip()
                    if key in payload:
                        if str(payload[key]) != val:
                            return False
                    elif f"data.{key}" in payload or key in str(payload.get("data", {})):
                        pass
                    else:
                        return False
            return True

    def get_dispatch_plan(self, event_type: str) -> list[dict[str, Any]]:
        """获取指定事件类型的完整分发计划（所有已生效规则）。"""
        rules = json_store.get_all(self.MODULE)
        plan = [
            {
                "rule_id": r["id"],
                "rule_no": r.get("规则编号"),
                "target": r.get("目标系统"),
                "method": r.get("分发方式"),
                "priority": r.get("优先级"),
                "on_failure": r.get("失败动作", "记录日志"),
                "filter": r.get("过滤条件"),
            }
            for r in rules
            if r.get("status") == "已生效"
            and str(r.get("事件类型", "")) in (event_type, "通用", "ALL", "*")
        ]

        priority_order = {"高": 0, "中": 1, "低": 2}
        plan.sort(key=lambda x: priority_order.get(str(x["priority"]), 2))

        return plan

    def domain_snapshot(self) -> dict[str, Any]:
        """输出事件分发路由的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        event_types = set(r.get("事件类型") for r in rows if r.get("事件类型"))
        targets = set(r.get("目标系统") for r in rows if r.get("目标系统"))

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "事件分发路由数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "unique_event_types": len(event_types),
            "unique_targets": len(targets),
            "operation": self.OPERATION_TERM,
        }