"""调用链路追踪领域服务：API调用日志记录、性能统计、异常分析与链路聚合。

记录每次API调用的完整链路信息，提供性能统计、慢调用分析和
异常分类能力。支持按端点、时间范围、状态进行聚合分析。
跨模块关联：api_endpoint（追踪端点）、integration_monitor（监控指标输出）。
"""
from __future__ import annotations

import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class CallTraceLogService(BaseModuleService):
    MODULE = "call_trace_log"
    ENTITY = "调用记录"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["记录编号", "关联端点", "请求参数", "响应码", "耗时毫秒", "数据量", "重试次数", "调用时间"]
    REQUIRED_FIELDS = ["记录编号", "关联端点", "请求参数"]
    STATUS_ORDER = ["正常", "慢调用", "异常", "已归档"]
    ACTION_RULES = {"标记慢调用": "慢调用", "标记异常": "异常", "归档记录": "已归档"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._slow_threshold_ms = 3000
        self._trace_buffer: list[dict[str, Any]] = []

    def _record_trace(self, endpoint_id: int, action: str, latency_ms: int,
                      status: str = "正常", record_count: int = 0) -> dict[str, Any]:
        """内部接口：记录一次调用追踪。

        自动根据延迟阈值判断是否慢调用。
        """
        now = datetime.now(timezone.utc)
        trace_id = str(uuid.uuid4())

        actual_status = status
        if status == "正常" and latency_ms > self._slow_threshold_ms:
            actual_status = "慢调用"

        existing = json_store.get_all(self.MODULE)
        max_id = max((int(e.get("id", 0)) for e in existing), default=0)

        record_no = f"TRACE-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{max_id + 1:05d}"

        trace_entry = {
            "id": max_id + 1,
            "记录编号": record_no,
            "关联端点": str(endpoint_id),
            "请求参数": f"[{action}] batch_size={record_count}",
            "响应码": "200" if actual_status == "正常" else "500",
            "耗时毫秒": str(latency_ms),
            "数据量": str(record_count),
            "重试次数": "0",
            "调用时间": now.isoformat(),
            "status": actual_status,
            "pending": actual_status != "已归档",
            "abnormal": actual_status == "异常",
            "created_at": now.isoformat(),
            "_trace_id": trace_id,
        }

        # 填充LIST_FIELDS
        for field in self.LIST_FIELDS:
            if field not in trace_entry:
                trace_entry[field] = ""

        json_store.insert(self.MODULE, trace_entry)

        self._trace_buffer.append({
            "trace_id": trace_id,
            "endpoint_id": endpoint_id,
            "action": action,
            "latency_ms": latency_ms,
            "status": actual_status,
            "timestamp": now.isoformat(),
        })

        return {"ok": True, "trace_id": trace_id, "record_id": trace_entry["id"]}

    def record_api_call(self, endpoint_id: int, action: str, latency_ms: int,
                        response_code: int = 200, record_count: int = 0,
                        request_params: str = "") -> dict[str, Any]:
        """记录一次API调用的完整追踪信息。"""
        return self._record_trace(endpoint_id, action, latency_ms,
                                  status="正常" if response_code < 400 else "异常",
                                  record_count=record_count)

    def get_performance_stats(self, endpoint_id: int | None = None,
                              hours: int = 24) -> dict[str, Any]:
        """获取API调用的性能统计。

        统计指标：
        - P50/P95/P99延迟
        - 调用总量、成功/失败比率
        - 异常分布
        """
        traces = json_store.get_all(self.MODULE)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        if endpoint_id is not None:
            traces = [t for t in traces if str(t.get("关联端点", "")) == str(endpoint_id)]

        traces = [t for t in traces if t.get("created_at", "")
                  and self._parse_time(t.get("created_at", "")) and self._parse_time(t.get("created_at", "")) > cutoff]

        if not traces:
            return {"total": 0, "message": "无数据"}

        latencies = []
        by_status = defaultdict(int)
        by_endpoint = defaultdict(int)
        total_records = 0

        for trace in traces:
            try:
                lat = int(str(trace.get("耗时毫秒", "0")))
                latencies.append(lat)
            except ValueError:
                latencies.append(0)

            by_status[str(trace.get("status", "正常"))] += 1
            by_endpoint[str(trace.get("关联端点", "unknown"))] += 1
            try:
                total_records += int(str(trace.get("数据量", "0")))
            except ValueError:
                pass

        latencies.sort()
        n = len(latencies)
        p50 = latencies[n // 2]
        p95 = latencies[int(n * 0.95)] if n > 20 else latencies[-1]
        p99 = latencies[int(n * 0.99)] if n > 100 else latencies[-1]

        return {
            "total": n,
            "total_records": total_records,
            "avg_latency_ms": round(sum(latencies) / n, 2),
            "p50_ms": p50,
            "p95_ms": p95,
            "p99_ms": p99,
            "max_ms": latencies[-1],
            "min_ms": latencies[0],
            "by_status": dict(by_status),
            "success_rate": round(by_status["正常"] / n * 100, 2) if n > 0 else 0,
            "by_endpoint": dict(sorted(by_endpoint.items(), key=lambda x: x[1], reverse=True)[:10]),
            "hours": hours,
        }

    def _parse_time(self, ts: str) -> datetime:
        """解析ISO时间戳。"""
        try:
            if "T" in ts:
                ts = ts.split("+")[0].split("Z")[0]
                return datetime.fromisoformat(ts).replace(tzinfo=timezone.utc)
            return datetime.strptime(ts, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return datetime(2000, 1, 1, tzinfo=timezone.utc)

    def _stats_by_endpoint(self, endpoint_id: int) -> dict[str, Any]:
        """内部接口：按端点统计调用情况（供api_endpoint等模块调用）。"""
        traces = json_store.get_all(self.MODULE)
        matching = [t for t in traces if str(t.get("关联端点", "")) == str(endpoint_id)]

        if not matching:
            return {"total": 0, "statuses": {}, "avg_latency": 0}

        latencies = []
        statuses = defaultdict(int)
        for trace in matching:
            try:
                latencies.append(int(str(trace.get("耗时毫秒", "0"))))
            except ValueError:
                pass
            statuses[str(trace.get("status", "正常"))] += 1

        return {
            "total": len(matching),
            "statuses": dict(statuses),
            "avg_latency_ms": round(sum(latencies) / max(len(latencies), 1), 2) if latencies else 0,
            "slow_calls": statuses.get("慢调用", 0),
            "errors": statuses.get("异常", 0),
        }

    def get_trace_buffer(self) -> list[dict[str, Any]]:
        """获取最近100条追踪缓冲。"""
        return self._trace_buffer[-100:]

    def domain_snapshot(self) -> dict[str, Any]:
        """输出调用链路追踪的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        perf = self.get_performance_stats(hours=24)
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "调用链路追踪数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_traces": perf.get("total", 0),
            "avg_latency_ms": perf.get("avg_latency_ms", 0),
            "success_rate": perf.get("success_rate", 0),
            "operation": self.OPERATION_TERM,
        }