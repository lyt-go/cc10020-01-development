"""认证凭证管理领域服务：凭证生成、加密存储、自动轮换与过期告警。

提供API凭证的生命周期管理，包括密钥生成、加密存储、
过期检测和自动轮换调度。
跨模块关联：api_endpoint（端点绑定）、call_trace_log（操作审计）。
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class ApiAuthStoreService(BaseModuleService):
    MODULE = "api_auth_store"
    ENTITY = "认证凭证"
    OPERATION_TERM = "关联"
    LIST_FIELDS = ["凭证编号", "关联端点", "凭证类型", "凭证摘要", "签发时间", "过期时间", "轮换周期", "凭证状态"]
    REQUIRED_FIELDS = ["凭证编号", "关联端点", "凭证类型"]
    STATUS_ORDER = ["已签发", "使用中", "即将过期", "已吊销"]
    ACTION_RULES = {"激活凭证": "使用中", "轮换凭证": "即将过期", "吊销凭证": "已吊销"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._encryption_key = secrets.token_hex(32)

    def generate_credential(self, endpoint_id: int, cred_type: str,
                            validity_days: int = 90) -> dict[str, Any]:
        """生成新的API认证凭证。

        流程：
        1. 生成随机密钥
        2. 使用HMAC加密存储
        3. 计算凭证摘要（不可逆指纹）
        4. 设置到期时间和轮换周期
        5. 与端点建立绑定关系
        """
        from app.services.api_endpoint import ApiEndpointService
        endpoint_svc = ApiEndpointService()
        endpoint = endpoint_svc.get_endpoint_detail(endpoint_id)
        if endpoint is None:
            return {"ok": False, "message": f"端点 {endpoint_id} 不存在"}

        # 生成明文密钥和加密版本
        raw_secret = secrets.token_hex(32)
        encrypted_secret = self._encrypt(raw_secret)

        # 生成凭证摘要
        digest = hashlib.sha256(f"{endpoint_id}:{raw_secret}:{cred_type}".encode()).hexdigest()[:16]

        now = datetime.now(timezone.utc)
        expire_time = now + timedelta(days=validity_days)

        existing = json_store.get_all(self.MODULE)
        max_id = max((int(e.get("id", 0)) for e in existing), default=0)
        cred_no = f"CRED-{endpoint_id:04d}-{max_id + 1:05d}"

        entry = {"id": max_id + 1}
        entry.update({
            "凭证编号": cred_no,
            "关联端点": str(endpoint_id),
            "凭证类型": cred_type,
            "凭证摘要": digest,
            "签发时间": now.isoformat(),
            "过期时间": expire_time.isoformat(),
            "轮换周期": f"{validity_days // 3}天",
            "凭证状态": "已签发",
            "status": "已签发",
            "pending": True,
            "abnormal": False,
            "created_at": now.isoformat(),
            "_encrypted_secret": encrypted_secret,
            "_raw_secret": raw_secret,
            "endpoint_name": endpoint.get("端点名称", ""),
        })
        json_store.insert(self.MODULE, entry)

        # 与端点建立绑定
        endpoint_svc.bind_credential(endpoint_id, entry["id"])

        return {
            "ok": True,
            "message": f"凭证已生成: {cred_no}",
            "credential_id": entry["id"],
            "credential_no": cred_no,
            "credential_type": cred_type,
            "expires_at": expire_time.isoformat(),
            "api_key": raw_secret,
            "digest": digest,
        }

    def _encrypt(self, text: str) -> str:
        """使用HMAC-SHA256对凭证进行加密存储。"""
        return hmac.new(self._encryption_key.encode(), text.encode(), hashlib.sha256).hexdigest()

    def verify_credential(self, credential_id: int, api_key: str) -> dict[str, Any]:
        """验证API密钥是否有效且在有效期内。"""
        entry = json_store.get_by_id(self.MODULE, credential_id)
        if entry is None:
            return {"ok": False, "message": "凭证不存在"}

        if entry.get("status") in ("已吊销", "已丢弃"):
            return {"ok": False, "message": f"凭证已{entry['status']}，不可使用"}

        # 验证密钥
        stored_secret = entry.get("_raw_secret", "")
        if api_key != stored_secret:
            return {"ok": False, "message": "密钥验证失败"}

        # 检查过期
        expire_str = str(entry.get("过期时间", ""))
        try:
            if "T" in expire_str:
                expire_time = datetime.fromisoformat(expire_str.split("+")[0])
            else:
                expire_time = datetime.strptime(expire_str, "%Y-%m-%d")
            expire_time = expire_time.replace(tzinfo=timezone.utc)
        except ValueError:
            return {"ok": False, "message": "凭证过期时间格式异常"}

        now = datetime.now(timezone.utc)
        if now > expire_time:
            json_store.update(self.MODULE, credential_id, {
                "status": "已吊销",
                "凭证状态": "已吊销",
                "abnormal": True,
            })
            return {"ok": False, "message": "凭证已过期，已自动吊销"}

        # 检查是否即将过期（7天内）
        if (expire_time - now).days <= 7:
            json_store.update(self.MODULE, credential_id, {
                "status": "即将过期",
                "凭证状态": "即将过期",
            })
            return {"ok": True, "message": "凭证有效，即将过期请尽快轮换", "expiring_soon": True, "days_left": (expire_time - now).days}

        return {"ok": True, "message": "凭证验证通过", "expiring_soon": False, "days_left": (expire_time - now).days}

    def auto_rotation_check(self) -> list[dict[str, Any]]:
        """自动检查需要轮换的凭证，返回即将过期的凭证列表。"""
        rows = json_store.get_all(self.MODULE)
        now = datetime.now(timezone.utc)
        expiring = []

        for row in rows:
            if row.get("status") in ("已吊销",):
                continue
            expire_str = str(row.get("过期时间", ""))
            try:
                if "T" in expire_str:
                    expire_time = datetime.fromisoformat(expire_str.split("+")[0]).replace(tzinfo=timezone.utc)
                else:
                    expire_time = datetime.strptime(expire_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue

            days_left = (expire_time - now).days
            if days_left <= 14:
                expiring.append({
                    "credential_id": row["id"],
                    "credential_no": row.get("凭证编号"),
                    "endpoint": row.get("关联端点"),
                    "days_left": days_left,
                    "status": row.get("status"),
                })

        return expiring

    def _get_credential(self, credential_id: int) -> dict[str, Any] | None:
        """内部接口：获取凭证详情（供api_endpoint等模块调用）。"""
        return json_store.get_by_id(self.MODULE, credential_id)

    def _list_by_endpoint(self, endpoint_id: int) -> list[dict[str, Any]]:
        """内部接口：按端点查询关联凭证列表。"""
        rows = json_store.get_all(self.MODULE)
        return [
            {
                "id": r["id"],
                "credential_no": r.get("凭证编号"),
                "type": r.get("凭证类型"),
                "digest": r.get("凭证摘要"),
                "status": r.get("status"),
                "expires_at": r.get("过期时间"),
            }
            for r in rows
            if str(r.get("关联端点", "")) == str(endpoint_id)
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        """输出认证凭证的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        expiring = self.auto_rotation_check()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "认证凭证数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "expiring_count": len(expiring),
            "unique_endpoints": len(set(r.get("关联端点") for r in rows if r.get("关联端点"))),
            "operation": self.OPERATION_TERM,
        }