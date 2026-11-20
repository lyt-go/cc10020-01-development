"""数据落湖领域服务：数据写入本地存储、分区组织与生命周期管理。

将接入的数据按日期/来源/类型分区写入本地JSON文件，
支持多种写入格式和压缩策略。
跨模块关联：webhook_channel（事件来源）、batch_ingest（批量来源）、event_archive（归档接口）。
"""
from __future__ import annotations

import hashlib
import json
import os
import uuid
import zlib
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class DataLakeSinkService(BaseModuleService):
    MODULE = "data_lake_sink"
    ENTITY = "落湖配置"
    OPERATION_TERM = "入湖"
    LIST_FIELDS = ["配置编号", "数据来源", "目标路径", "分区策略", "写入格式", "压缩算法", "保留天数", "落湖水位"]
    REQUIRED_FIELDS = ["配置编号", "数据来源", "目标路径"]
    STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
    ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用配置": "已停用"}
    NEGATIVE_ACTIONS = ["停用配置"]

    IDENTITY_MODULE = "lake_sink_identity"

    def __init__(self, lake_base: str | None = None) -> None:
        super().__init__()
        if lake_base is None:
            lake_base = os.environ.get("LAKE_BASE_DIR") or os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                "data_store", "lake",
            )
        self._lake_base = lake_base
        os.makedirs(self._lake_base, exist_ok=True)
        self._write_stats: dict[str, int] = {}

    def find_sink_receipt(self, source_row_key: str) -> dict[str, Any] | None:
        """按来源行身份查正式落湖回执；查不到即说明该来源行尚未落湖。"""
        for row in json_store.get_all(self.IDENTITY_MODULE):
            if row.get("source_row_key") == source_row_key:
                return row
        return None

    def _register_identity(
        self,
        source_row_key: str | None,
        event_id: str,
        event_type: str,
        receipt: dict[str, Any],
        source_channel_id: int,
    ) -> dict[str, Any]:
        """登记来源行身份与落湖回执；同一身份只允许一条正式落湖记录。"""
        if not source_row_key:
            return receipt
        existing = self.find_sink_receipt(source_row_key)
        if existing is not None:
            return {
                "ok": True,
                "duplicate": True,
                "message": f"来源行 {source_row_key} 已落湖，跳过重复写入",
                "partition": existing.get("partition", ""),
                "file_path": existing.get("file_path", ""),
                "record_hash": existing.get("record_hash", ""),
                "event_id": existing.get("event_id"),
                "source_row_key": source_row_key,
            }
        json_store.insert(self.IDENTITY_MODULE, {
            "source_row_key": source_row_key,
            "event_id": event_id,
            "event_type": event_type,
            "source_channel_id": source_channel_id,
            "partition": receipt.get("partition", ""),
            "file_path": receipt.get("file_path", ""),
            "record_hash": receipt.get("record_hash", ""),
            "sunk_at": datetime.now(timezone.utc).isoformat(),
        })
        receipt["source_row_key"] = source_row_key
        return receipt

    def write_event(self, event_id: str, event_type: str, payload: dict[str, Any],
                    source_channel_id: int = 0,
                    source_row_key: str | None = None) -> dict[str, Any]:
        """将事件数据写入数据湖。

        写入流程：
        1. 若携带来源行身份，先查重：同一来源行只允许一条正式落湖记录
        2. 确定分区路径（按日期/事件类型）
        3. 序列化数据（默认JSON格式）
        4. 写入分区文件并持久化来源行身份与回执

        Args:
            event_id: 事件唯一标识
            event_type: 事件类型
            payload: 事件数据
            source_channel_id: 来源通道ID
            source_row_key: 来源行身份键，用于跨批次幂等

        Returns:
            { ok: bool, message: str, partition: str, file_path: str }
        """
        if source_row_key:
            existing = self.find_sink_receipt(source_row_key)
            if existing is not None:
                return {
                    "ok": True,
                    "duplicate": True,
                    "message": f"来源行 {source_row_key} 已落湖，跳过重复写入",
                    "partition": existing.get("partition", ""),
                    "file_path": existing.get("file_path", ""),
                    "record_hash": existing.get("record_hash", ""),
                    "event_id": existing.get("event_id"),
                    "source_row_key": source_row_key,
                }

        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y/%m/%d")
        partition_path = os.path.join(self._lake_base, f"events/{event_type}", date_partition)
        os.makedirs(partition_path, exist_ok=True)

        # 构建数据记录
        record = {
            "event_id": event_id,
            "event_type": event_type,
            "source_channel_id": source_channel_id,
            "payload": payload,
            "ingested_at": now.isoformat(),
            "record_hash": hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:16],
        }

        # 写入分区文件：时间戳 + 事件片段 + 随机后缀，避免同秒多事件互相覆盖
        file_name = f"{now.strftime('%H%M%S')}_{event_id[:8]}_{uuid.uuid4().hex[:8]}.json"
        file_path = os.path.join(partition_path, file_name)

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, default=str)

            self._write_stats[event_type] = self._write_stats.get(event_type, 0) + 1

            receipt = {
                "ok": True,
                "message": f"数据已写入数据湖分区 {date_partition}",
                "partition": f"events/{event_type}/{date_partition}",
                "file_path": file_path,
                "record_hash": record["record_hash"],
                "duplicate": False,
            }
            return self._register_identity(
                source_row_key, event_id, event_type, receipt, source_channel_id
            )
        except OSError as e:
            return {"ok": False, "message": f"写入失败: {e}", "partition": "", "file_path": ""}

    def write_batch(self, records: list[dict[str, Any]], event_type: str = "batch",
                    source_channel_id: int = 0) -> dict[str, Any]:
        """批量写入多条记录到数据湖。

        按每小时一个文件聚合写入。
        """
        if not records:
            return {"ok": True, "message": "无数据需要写入", "written": 0}

        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y/%m/%d")
        partition_path = os.path.join(self._lake_base, f"events/{event_type}", date_partition)
        os.makedirs(partition_path, exist_ok=True)

        batch_file = f"batch_{now.strftime('%H%M%S')}.json"
        file_path = os.path.join(partition_path, batch_file)

        batch_records = []
        for i, record in enumerate(records):
            batch_records.append({
                "event_id": f"batch_{now.strftime('%Y%m%d%H%M%S')}_{i:04d}",
                "event_type": event_type,
                "source_channel_id": source_channel_id,
                "payload": record,
                "ingested_at": now.isoformat(),
            })

        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(batch_records, f, ensure_ascii=False, default=str)

            self._write_stats[event_type] = self._write_stats.get(event_type, 0) + len(batch_records)

            return {
                "ok": True,
                "message": f"批量 {len(batch_records)} 条写入完成",
                "written": len(batch_records),
                "partition": f"events/{event_type}/{date_partition}",
                "file_path": file_path,
            }
        except OSError as e:
            return {"ok": False, "message": f"批量写入失败: {e}", "written": 0}

    def find_lake_record(self, source_row_key: str) -> dict[str, Any] | None:
        """按来源行身份核对湖里是否存在正式落湖记录。

        阻断行从未调用写入，重复写入也不会产生第二份文件，因此该方法
        对阻断身份始终返回 None，合格身份最多返回一条记录。
        """
        receipt = self.find_sink_receipt(source_row_key)
        if receipt is None:
            return None
        file_path = receipt.get("file_path", "")
        if not file_path or not os.path.exists(file_path):
            return None
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            return None
        records = data if isinstance(data, list) else [data]
        for rec in records:
            payload = rec.get("payload", {})
            if isinstance(payload, dict) and payload.get("_source_row_key") == source_row_key:
                return rec
            if rec.get("event_id") == receipt.get("event_id"):
                return rec
        return records[0] if records else None

    def get_identity_stats(self) -> dict[str, Any]:
        """落湖身份注册统计：用于核对同一来源行是否被累计写入两次。"""
        rows = json_store.get_all(self.IDENTITY_MODULE)
        return {
            "total_identities": len(rows),
            "unique_source_rows": len({r.get("source_row_key") for r in rows}),
            "duplicate_skips": int(self._write_stats.get("__duplicate_skips__", 0)),
        }

    def query_lake_data(self, event_type: str, date_str: str | None = None,
                        limit: int = 100) -> dict[str, Any]:
        """查询数据湖中的指定分区数据。"""
        if date_str is None:
            date_str = datetime.now(timezone.utc).strftime("%Y/%m/%d")

        partition_path = os.path.join(self._lake_base, f"events/{event_type}", date_str)

        if not os.path.exists(partition_path):
            return {"ok": True, "records": [], "total": 0, "partition": f"events/{event_type}/{date_str}"}

        records = []
        for fname in sorted(os.listdir(partition_path))[:limit]:
            if fname.endswith(".json"):
                try:
                    with open(os.path.join(partition_path, fname), "r", encoding="utf-8") as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            records.extend(data)
                        else:
                            records.append(data)
                except (json.JSONDecodeError, OSError):
                    continue

        return {
            "ok": True,
            "records": records[:limit],
            "total": len(records),
            "partition": f"events/{event_type}/{date_str}",
        }

    def get_lake_stats(self) -> dict[str, Any]:
        """获取数据湖存储统计信息。"""
        total_files = 0
        total_size = 0
        by_type: dict[str, int] = {}

        for root, dirs, files in os.walk(self._lake_base):
            for f in files:
                if f.endswith(".json"):
                    total_files += 1
                    filepath = os.path.join(root, f)
                    try:
                        total_size += os.path.getsize(filepath)
                    except OSError:
                        pass

                # 按事件类型统计
                parts = root.split(os.sep)
                if "events" in parts:
                    idx = parts.index("events")
                    if len(parts) > idx + 1:
                        etype = parts[idx + 1]
                        by_type[etype] = by_type.get(etype, 0) + 1

        return {
            "base_dir": self._lake_base,
            "total_files": total_files,
            "total_size_bytes": total_size,
            "total_size_mb": round(total_size / (1024 * 1024), 2),
            "by_type": dict(sorted(by_type.items(), key=lambda x: x[1], reverse=True)),
            "write_stats": self._write_stats,
        }

    def get_partition_info(self) -> list[dict[str, Any]]:
        """获取所有分区信息。"""
        partitions = []
        for root, dirs, files in os.walk(self._lake_base):
            json_files = [f for f in files if f.endswith(".json")]
            if json_files:
                rel_path = os.path.relpath(root, self._lake_base)
                total = 0
                for f in json_files:
                    try:
                        total += os.path.getsize(os.path.join(root, f))
                    except OSError:
                        pass
                partitions.append({
                    "path": rel_path,
                    "file_count": len(json_files),
                    "size_bytes": total,
                })
        return partitions

    def domain_snapshot(self) -> dict[str, Any]:
        """输出数据落湖的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        lake_stats = self.get_lake_stats()
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "数据落湖数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "lake_files": lake_stats.get("total_files", 0),
            "lake_size_mb": lake_stats.get("total_size_mb", 0),
            "sunk_identities": len(json_store.get_all(self.IDENTITY_MODULE)),
            "operation": self.OPERATION_TERM,
        }