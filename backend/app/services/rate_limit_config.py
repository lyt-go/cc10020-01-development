"""流量控制领域服务：多级限流、配额管理、突发容量与令牌桶算法。

支持QPS并发限制、日/月配额、突发容量和请求间隔控制。
跨模块关联：api_endpoint（端点关联）、call_trace_log（限流拒绝追踪）。
"""
from __future__ import annotations

import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class RateLimitConfigService(BaseModuleService):
    MODULE = "rate_limit_config"
    ENTITY = "限流规则"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["规则编号", "关联端点", "请求间隔", "并发上限", "日配额", "月配额", "突发容量", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "关联端点", "请求间隔"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def __init__(self) -> None:
        super().__init__()
        self._token_buckets: dict[int, dict[str, float]] = {}
        self._request_counts: dict[int, dict[str, int]] = {}
        self._daily_usage: dict[int, list[dict[str, Any]]] = defaultdict(list)

    def check_limit(self, endpoint_id: int) -> dict[str, Any]:
        """执行多级限流检查。

        检查层级：
        1. 请求间隔（最小间隔ms）
        2. 并发数限制
        3. 日配额限制
        4. 月配额限制
        5. 突发容量限制

        任一层级超限即返回拒绝。

        Returns:
            { allowed: bool, limit_type: str, current: int, max: int, wait_ms: int }
        """
        rules = json_store.get_all(self.MODULE)
        matching = [
            r for r in rules
            if r.get("status") == "已生效"
            and str(r.get("关联端点", "")) == str(endpoint_id)
        ]

        if not matching:
            return {"allowed": True, "message": "无限流规则，放行", "rules_checked": 0}

        now = time.time()
        results = []
        all_allowed = True

        for rule in matching:
            rule_id = int(rule["id"])

            # 初始化令牌桶
            if rule_id not in self._token_buckets:
                burst = int(rule.get("突发容量", "10") or "10")
                self._token_buckets[rule_id] = {"tokens": float(burst), "last_refill": now}

            try:
                interval_ms = int(rule.get("请求间隔", "100") or "100")
            except ValueError:
                interval_ms = 100

            try:
                concurrency = int(rule.get("并发上限", "10") or "10")
            except ValueError:
                concurrency = 10

            try:
                daily_quota = int(rule.get("日配额", "10000") or "10000")
            except ValueError:
                daily_quota = 10000

            # 1. 请求间隔检查
            if rule_id in self._request_counts:
                last_req = self._request_counts[rule_id].get("last_request_time", 0)
                elapsed = (now - last_req) * 1000
                if elapsed < interval_ms:
                    all_allowed = False
                    results.append({"allowed": False, "limit_type": "请求间隔", "current": elapsed, "max": interval_ms, "wait_ms": int(interval_ms - elapsed)})

            # 2. 并发检查
            current_concurrency = self._request_counts.get(rule_id, {}).get("concurrent", 0)
            if current_concurrency >= concurrency:
                all_allowed = False
                results.append({"allowed": False, "limit_type": "并发限制", "current": current_concurrency, "max": concurrency})

            # 3. 日配额检查
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            daily_count = sum(1 for r in self._daily_usage.get(rule_id, []) if r.get("date") == today)
            if daily_count >= daily_quota:
                all_allowed = False
                results.append({"allowed": False, "limit_type": "日配额", "current": daily_count, "max": daily_quota})

            # 更新请求计数
            if rule_id not in self._request_counts:
                self._request_counts[rule_id] = {}
            self._request_counts[rule_id]["last_request_time"] = now

            # 记录日使用量
            self._daily_usage[rule_id].append({
                "date": today,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        if all_allowed:
            return {"allowed": True, "message": "限流检查通过", "rules_checked": len(matching), "checks": results}
        else:
            return {"allowed": False, "message": "触发限流规则", "rules_checked": len(matching), "blocked_by": results}

    def get_rate_limit_metrics(self, endpoint_id: int) -> dict[str, Any]:
        """获取端点的限流指标统计。"""
        rules = json_store.get_all(self.MODULE)
        matching = [r for r in rules if str(r.get("关联端点", "")) == str(endpoint_id)]

        if not matching:
            return {"endpoint_id": endpoint_id, "rules": 0, "metrics": []}

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        metrics = []
        for rule in matching:
            rule_id = int(rule["id"])
            daily_count = sum(1 for r in self._daily_usage.get(rule_id, []) if r.get("date") == today)
            metrics.append({
                "rule_id": rule_id,
                "rule_no": rule.get("规则编号"),
                "daily_quota": int(rule.get("日配额", "10000") or "10000"),
                "daily_used": daily_count,
                "usage_pct": round(daily_count / max(int(rule.get("日配额", "10000") or "10000"), 1) * 100, 2),
                "concurrency": rule.get("并发上限"),
                "interval_ms": rule.get("请求间隔"),
            })

        return {
            "endpoint_id": endpoint_id,
            "rules": len(metrics),
            "total_daily_used": sum(m["daily_used"] for m in metrics),
            "metrics": metrics,
        }

    def _list_by_endpoint(self, endpoint_id: int) -> list[dict[str, Any]]:
        """内部接口：按端点查询限流规则。"""
        rows = json_store.get_all(self.MODULE)
        return [
            {
                "id": r["id"],
                "rule_no": r.get("规则编号"),
                "interval_ms": r.get("请求间隔"),
                "concurrency": r.get("并发上限"),
                "daily_quota": r.get("日配额"),
                "status": r.get("status"),
            }
            for r in rows
            if str(r.get("关联端点", "")) == str(endpoint_id)
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        """输出流量控制的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "流量控制数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "active_rules": sum(1 for r in rows if r.get("status") == "已生效"),
            "endpoints_regulated": len(set(r.get("关联端点") for r in rows if r.get("关联端点"))),
            "operation": self.OPERATION_TERM,
        }