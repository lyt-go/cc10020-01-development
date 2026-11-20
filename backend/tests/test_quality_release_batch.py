"""质检放行清单驱动的批量接入测试：放行、阻断、中断续做与跨批次幂等。

覆盖需求要点：
1. 同页转换数据批量校验产出质检放行清单（可落湖记录 + 阻断原因）
2. 批量写入中断后按持久化的来源行身份关联放行清单与回执续做
3. 两轮批次重叠只写入一份合格行；阻断行始终查不到正式落湖记录
4. 批量接入调度跨模块消费放行清单，按可写行生成接入结算清单，
   已处理数来自数据落湖的实际回执
5. 同页混有正常行与阻断行时，放行边界决定湖里已可用的数据
6. 批量写入停在一半时，可核对同一来源行有没有累计两次
"""
from __future__ import annotations

import importlib
import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.persistence import JsonStore, json_store as global_store
import app.persistence as persistence_mod
import app.services.data_quality_rule as dq_mod
import app.services.data_lake_sink as sink_mod
import app.services.batch_ingest as bi_mod
import app.services.incre_cursor as ic_mod


@pytest.fixture()
def isolated_ctx(monkeypatch):
    """每个用例独立的 JSON 存储与湖目录，避免跨用例污染。"""
    with tempfile.TemporaryDirectory() as tmp:
        store = JsonStore(base_dir=tmp)
        monkeypatch.setattr(persistence_mod, "json_store", store)
        lake_dir = os.path.join(tmp, "lake")
        monkeypatch.setenv("LAKE_BASE_DIR", lake_dir)
        for mod in (dq_mod, sink_mod, bi_mod, ic_mod):
            importlib.reload(mod)
        yield store, lake_dir
        monkeypatch.setattr(persistence_mod, "json_store", global_store)


def _create_running_task(store, endpoint_id: int = 1, checkpoint: str = "是") -> int:
    task = store.insert("batch_ingest", {
        "任务编号": "BI-QRL-001",
        "关联端点": str(endpoint_id),
        "Cron表达式": "0 */1 * * *",
        "分页策略": "分页拉取",
        "并行批次": "2",
        "断点续传": checkpoint,
        "失败动作": "记录并继续",
        "任务版本": "v1",
        "status": "运行中",
        "pending": False,
        "abnormal": False,
    })
    return task["id"]


def _add_blocking_rule(store, field: str = "value", min_value: float = 50.0) -> None:
    rule = store.insert("data_quality_rule", {
        "规则编号": "QR-BLOCK-001",
        "数据来源": "batch_ingest",
        "校验字段": field,
        "校验表达式": "is_numeric",
        "阈值下界": str(min_value),
        "阈值上界": "0",
        "阻断策略": "阻断",
        "规则版本": "v1",
        "status": "已生效",
        "pending": False,
        "abnormal": False,
    })
    return rule["id"]


def _page(page: int, size: int, low_value_indexes: tuple[int, ...]) -> list[dict]:
    return [
        {
            "id": (page - 1) * size + i + 1,
            "name": f"record_p{page}_{i}",
            "value": 10 if i in low_value_indexes else 100,
            "status": "active",
        }
        for i in range(size)
    ]


# 1. 同页批量校验：放行清单直接给出可落湖记录与阻断原因
def test_release_list_marks_releasable_and_blocked_rows(isolated_ctx):
    store, _ = isolated_ctx
    task_id = _create_running_task(store)
    _add_blocking_rule(store)

    svc = bi_mod.BatchIngestService()
    result = svc.execute_batch(
        task_id, max_pages=1, page_size=6,
        injected_pages={1: _page(1, 6, low_value_indexes=(1, 4))},
    )

    assert result["ok"]
    assert result["records_fetched"] == 6
    assert result["records_sunk"] == 4
    assert result["records_blocked"] == 2

    quality = dq_mod.DataQualityRuleService()
    lists = quality.get_release_lists(task_id=task_id)
    assert len(lists) == 1
    release = lists[0]
    assert release["放行数"] == 4
    assert release["阻断数"] == 2
    blocked_items = [it for it in release["items"] if it["放行结论"] == "阻断"]
    assert all(it["阻断原因"] for it in blocked_items)
    assert all("value" in it["阻断原因"] for it in blocked_items)


# 2. 阻断行始终查不到正式落湖记录
def test_blocked_rows_never_reach_lake(isolated_ctx):
    store, _ = isolated_ctx
    task_id = _create_running_task(store)
    _add_blocking_rule(store)

    svc = bi_mod.BatchIngestService()
    svc.execute_batch(
        task_id, max_pages=1, page_size=5,
        injected_pages={1: _page(1, 5, low_value_indexes=(2,))},
    )

    sink = sink_mod.DataLakeSinkService()
    quality = dq_mod.DataQualityRuleService()
    release = quality.get_release_lists(task_id=task_id)[0]

    for item in release["items"]:
        key = item["来源行身份"]
        if item["放行结论"] == "阻断":
            assert sink.find_sink_receipt(key) is None
            assert sink.find_lake_record(key) is None
        else:
            assert sink.find_lake_record(key) is not None

    lake = sink.query_lake_data("batch_ingest")
    assert lake["total"] == 4


# 3. 同页放行边界决定湖里已经可用的数据
def test_page_release_boundary_reflects_lake_availability(isolated_ctx):
    store, _ = isolated_ctx
    task_id = _create_running_task(store)
    _add_blocking_rule(store)

    svc = bi_mod.BatchIngestService()
    result = svc.execute_batch(
        task_id, max_pages=1, page_size=4,
        injected_pages={1: _page(1, 4, low_value_indexes=(0, 3))},
    )
    batch_id = result["batch_id"]

    boundary = svc.get_page_release_boundary(task_id, batch_id, 1)
    assert boundary["ok"]
    assert boundary["放行数"] == 2
    assert boundary["阻断数"] == 2
    assert boundary["湖里可用数"] == 2
    assert boundary["页关闭"] is True
    for row in boundary["rows"]:
        assert row["湖里可用"] == (row["放行结论"] == "放行")


# 4. 批量写入停在一半：中断持久化，续做后只补写待处置行
def test_interrupted_write_resumes_by_source_row_identity(isolated_ctx):
    store, _ = isolated_ctx
    task_id = _create_running_task(store)
    _add_blocking_rule(store)

    svc = bi_mod.BatchIngestService()
    first = svc.execute_batch(
        task_id, max_pages=2, page_size=4,
        injected_pages={
            1: _page(1, 4, low_value_indexes=(1,)),  # 3 放行 + 1 阻断
            2: _page(2, 4, low_value_indexes=()),     # 4 放行
        },
        interrupt_after=2,  # 写满 2 条后中断，后续页已在注入数据里可拉取
    )

    assert first["interrupted"] is True
    assert first["records_sunk"] == 2
    batch_id = first["batch_id"]

    # 检查点与待处置行均已持久化
    checkpoint = svc._load_checkpoint(task_id)
    assert checkpoint["批次号"] == batch_id
    assert checkpoint["中断标记"] is True
    settlement = svc._find_settlement(task_id, batch_id, 1)
    assert settlement["待处置数"] == 1
    assert settlement["已处理数"] == 2

    # 续做：复用批次号与放行清单，仅补写剩余待处置放行行
    second = svc.execute_batch(
        task_id, max_pages=2, page_size=4,
        injected_pages={
            1: _page(1, 4, low_value_indexes=(1,)),
            2: _page(2, 4, low_value_indexes=()),
        },
    )
    assert second["interrupted"] is not True
    assert second["batch_id"] == batch_id
    # 第1页补写 1 条 + 第2页 4 条
    assert second["records_sunk"] == 5
    assert second["records_blocked"] == 1

    # 已写行不会二次落湖：两轮回执总数 = 全部放行行
    audit = svc.reconcile_source_rows(task_id)
    assert audit["ok"] is True
    assert audit["回执总数"] == 7
    assert audit["结算已处理数"] == 7
    assert audit["待处置行数"] == 0
    assert audit["违规明细"] == []

    sink = sink_mod.DataLakeSinkService()
    assert sink.query_lake_data("batch_ingest")["total"] == 7
    # 断点闭环后清理
    assert svc._load_checkpoint(task_id) is None


# 5. 两轮批次重叠：同一来源行只落湖一份
def test_overlapping_batches_write_each_qualified_row_once(isolated_ctx):
    store, _ = isolated_ctx
    task_id = _create_running_task(store)
    _add_blocking_rule(store)

    svc = bi_mod.BatchIngestService()
    first = svc.execute_batch(
        task_id, max_pages=1, page_size=4,
        injected_pages={1: _page(1, 4, low_value_indexes=(2,))},
    )
    first_batch = first["batch_id"]
    assert first["records_sunk"] == 3

    # 第二轮批次从第1页重新拉取（重叠），身份相同
    second = svc.execute_batch(
        task_id, max_pages=1, page_size=4,
        injected_pages={1: _page(1, 4, low_value_indexes=(2,))},
    )
    assert second["records_sunk"] == 0
    assert second["duplicates_skipped"] == 3
    assert second["records_blocked"] == 1

    sink = sink_mod.DataLakeSinkService()
    # 湖里合格行仍然只有一份
    assert sink.query_lake_data("batch_ingest")["total"] == 3

    audit = svc.reconcile_source_rows(task_id)
    assert audit["ok"] is True
    identities = [r["来源行身份"]
                  for s in svc.get_settlement_lists(task_id)
                  for r in s["rows"] if r["结论"] == "放行"]
    # 两个批次的结算清单都保留，但来源行身份一致
    assert len(identities) == 6
    assert len(set(identities)) == 3

    # 阻断身份在两轮清单里都是阻断，且查不到正式落湖记录
    quality = dq_mod.DataQualityRuleService()
    blocked_keys = [
        it["来源行身份"]
        for lst in quality.get_release_lists(task_id=task_id)
        for it in lst["items"] if it["放行结论"] == "阻断"
    ]
    assert blocked_keys and len(set(blocked_keys)) == 1
    for key in set(blocked_keys):
        assert sink.find_lake_record(key) is None


# 6. 接入结算清单：已处理数来自落湖实际回执
def test_settlement_processed_count_matches_sink_receipts(isolated_ctx):
    store, _ = isolated_ctx
    task_id = _create_running_task(store)
    _add_blocking_rule(store)

    svc = bi_mod.BatchIngestService()
    svc.execute_batch(
        task_id, max_pages=2, page_size=4,
        injected_pages={
            1: _page(1, 4, low_value_indexes=(0,)),
            2: _page(2, 4, low_value_indexes=(3,)),
        },
    )

    settlements = svc.get_settlement_lists(task_id=task_id)
    assert len(settlements) == 2
    total_processed = 0
    for settlement in settlements:
        assert settlement["页关闭"] is True
        assert settlement["放行数"] + settlement["阻断数"] == 4
        assert settlement["已处理数"] == settlement["放行数"]
        total_processed += settlement["已处理数"]
        for row in settlement["rows"]:
            if row["结论"] == "放行":
                assert row["回执"] is not None
                assert row["回执"]["file_path"]
                assert row["状态"] in ("已落湖", "重复跳过")
            else:
                assert row["回执"] is None
                assert row["状态"] == "阻断"
    assert total_processed == 6

    progress = svc.get_task_progress(task_id)
    assert progress["settlement_pages"] == 2
    assert progress["settled_processed"] == 6
    assert progress["settled_blocked"] == 2


# 7. 放行清单幂等：同页重复构建复用持久化清单
def test_release_list_is_idempotent_for_same_page(isolated_ctx):
    store, _ = isolated_ctx
    task_id = _create_running_task(store)
    _add_blocking_rule(store)

    quality = dq_mod.DataQualityRuleService()
    records = _page(1, 4, low_value_indexes=(1,))
    first = quality.build_release_list(task_id, "BATCH-X", 1, records)
    second = quality.build_release_list(task_id, "BATCH-X", 1, records)

    assert first["清单编号"] == second["清单编号"]
    assert second["reused"] is True
    assert len(quality.get_release_lists(task_id, "BATCH-X", 1)) == 1

    decision = quality.get_release_decision(first["items"][0]["来源行身份"], task_id)
    assert decision is not None
    assert decision["放行结论"] in ("放行", "阻断")


# 8. 无阻断规则时全部放行，核对无违规
def test_all_released_when_no_blocking_rule(isolated_ctx):
    store, _ = isolated_ctx
    task_id = _create_running_task(store)

    svc = bi_mod.BatchIngestService()
    result = svc.execute_batch(
        task_id, max_pages=1, page_size=5,
        injected_pages={1: _page(1, 5, low_value_indexes=())},
    )
    assert result["records_sunk"] == 5
    assert result["records_blocked"] == 0
    audit = svc.reconcile_source_rows(task_id)
    assert audit["ok"] is True
    assert audit["阻断行数"] == 0
