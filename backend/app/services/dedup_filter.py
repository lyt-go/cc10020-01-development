"""事件去重领域服务：幂等去重、时间窗口去重与重复判定。

支持基于事件ID的精确去重、基于内容指纹的模糊去重和
时间窗口内的重复事件识别。
跨模块关联：webhook_channel（事件接收去重）、data_lake_sink（存储去重检查）。
"""
from __future__ import annotations

import hashlib
import json
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class DedupFilterService(BaseModuleService):
    MODULE = "dedup_filter"
    ENTITY = "去重规则"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["规则编号", "事件类型", "去重键列", "比较方式", "时间窗口", "相似度阈值", "冲突策略", "规则版本"]
    REQUIRED_FIELDS = ["规则编号", "事件类型", "去重键列"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    def __init__(self) -> None:
        super().__init__()
        self._fingerprint_cache: dict[str, float] = {}
        self._idempotency_keys: set[str] = set()
        self._duplicate_stats: dict[str, int] = defaultdict(int)

    def check_duplicate(self, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        """检查事件是否重复。

        去重策略（按规则优先级）：
        1. 幂等键去重：基于事件唯一标识
        2. 内容指纹去重：基于payload内容哈希
        3. 时间窗口去重：相同类型事件在时间窗口内重复

        Returns:
            { ok: bool, is_duplicate: bool, reason: str, dedup_key: str }
        """
        rules = json_store.get_all(self.MODULE)
        matching = [
            r for r in rules
            if r.get("status") == "已生效"
            and str(r.get("事件类型", "")) in (event_type, "通用", "ALL", "*")
        ]

        if not matching:
            return {"ok": True, "is_duplicate": False, "reason": "无匹配去重规则"}

        for rule in matching:
            dedup_keys = str(rule.get("去重键列", "id"))
            compare_method = str(rule.get("比较方式", "精确匹配"))
            threshold = float(rule.get("相似度阈值", "0.95") or "0.95")
            conflict_policy = str(rule.get("冲突策略", "跳过"))

            # 构建去重键
            dedup_key = self._build_dedup_key(event_type, payload, dedup_keys)

            if not dedup_key:
                continue

            # 幂等检查
            if dedup_key in self._idempotency_keys:
                self._duplicate_stats[event_type] += 1
                return {
                    "ok": True,
                    "is_duplicate": True,
                    "reason": f"幂等键重复: {dedup_key[:32]}...",
                    "dedup_key": dedup_key,
                    "policy": conflict_policy,
                }

            # 内容指纹检查
            if compare_method in ("模糊匹配", "相似度匹配"):
                fp = self._compute_content_fingerprint(payload)
                now = time.time()
                window_hours = self._parse_time_window(str(rule.get("时间窗口", "24")))
                for cached_fp, cached_time in list(self._fingerprint_cache.items()):
                    if (now - cached_time) < window_hours * 3600:
                        similarity = self._compute_similarity(fp, cached_fp)
                        if similarity >= threshold:
                            self._duplicate_stats[event_type] += 1
                            return {
                                "ok": True,
                                "is_duplicate": True,
                                "reason": f"内容相似度 {similarity:.2f} >= {threshold}",
                                "dedup_key": dedup_key,
                                "similarity": similarity,
                                "policy": conflict_policy,
                            }
                self._fingerprint_cache[fp] = now

            # 标记为已处理
            self._idempotency_keys.add(dedup_key)

        return {"ok": True, "is_duplicate": False, "reason": "通过去重检查"}

    def _build_dedup_key(self, event_type: str, payload: dict[str, Any], key_fields: str) -> str:
        """构建去重唯一键。

        支持单键和多键组合（逗号分隔）。
        """
        if not key_fields:
            return ""

        parts = []
        for key in key_fields.split(","):
            key = key.strip()
            if key == "id":
                val = payload.get("id") or payload.get("event_id", "")
                parts.append(str(val))
            elif key in payload:
                parts.append(str(payload[key]))
            elif f"data.{key}" in str(payload.get("data", {})):
                parts.append(str(payload.get("data", {}).get(key, "")))
            else:
                for dk, dv in payload.items():
                    if dk == key:
                        parts.append(str(dv))
                        break

        parts.insert(0, event_type)
        return "|".join(parts)

    def _compute_content_fingerprint(self, payload: dict[str, Any]) -> str:
        """计算payload的内容指纹。"""
        content = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(content.encode()).hexdigest()

    def _compute_similarity(self, fp1: str, fp2: str) -> float:
        """计算两个指纹的相似度（基于字符重合度）。"""
        if fp1 == fp2:
            return 1.0
        common = sum(1 for a, b in zip(fp1, fp2) if a == b)
        return common / max(len(fp1), len(fp2))

    def _parse_time_window(self, window_str: str) -> float:
        """解析时间窗口字符串为小时数。"""
        import re
        match = re.match(r"(\d+)\s*(小时|分钟|天|h|m|d)", window_str, re.I)
        if match:
            value = int(match.group(1))
            unit = match.group(2).lower()
            if unit in ("分钟", "m"):
                return value / 60
            elif unit in ("天", "d"):
                return value * 24
            return float(value)
        return 24.0

    def get_dedup_stats(self) -> dict[str, Any]:
        """获取去重统计信息。"""
        return {
            "total_fingerprints": len(self._fingerprint_cache),
            "total_idempotency_keys": len(self._idempotency_keys),
            "duplicates_detected": dict(self._duplicate_stats),
            "total_duplicates": sum(self._duplicate_stats.values()),
        }

    def clear_dedup_cache(self) -> dict[str, Any]:
        """清空去重缓存（用于周期性重置）。"""
        total_dup = sum(self._duplicate_stats.values())
        self._fingerprint_cache.clear()
        self._idempotency_keys.clear()
        self._duplicate_stats.clear()
        return {"ok": True, "message": f"去重缓存已清空（历史去重: {total_dup}）"}

    def domain_snapshot(self) -> dict[str, Any]:
        """输出事件去重的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "事件去重数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "cache_size": len(self._fingerprint_cache),
            "total_duplicates_blocked": sum(self._duplicate_stats.values()),
            "operation": self.OPERATION_TERM,
        }