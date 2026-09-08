"""DAG 完整性测试：确保所有 DAG 能被正常解析，拓扑/重试/血缘符合设计。"""

import pytest
from airflow.models import DagBag

EXPECTED_DAGS = {
    "datamart_daily_etl",
    "datamart_daily_report",
    "datamart_failure_demo",
    "datamart_deadline_demo",
    "datamart_incremental_etl",
    "datamart_backfill",
}


@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(dag_folder="dags")


def test_no_import_errors(dag_bag):
    assert not dag_bag.import_errors, f"DAG 导入错误: {dag_bag.import_errors}"


def test_all_dags_loaded(dag_bag):
    assert set(dag_bag.dags) >= EXPECTED_DAGS


def test_etl_dag_structure(dag_bag):
    dag = dag_bag.dags["datamart_daily_etl"]
    # 并行分支：start 的下游是抽取分组内的两个任务
    start_downstream = {t.task_id for t in dag.get_task("start").downstream_list}
    assert start_downstream == {"extract.extract_orders", "extract.extract_users"}
    # 汇聚依赖：transform 的两个上游
    transform_upstream = {t.task_id for t in dag.get_task("transform").upstream_list}
    assert transform_upstream == {"extract.extract_orders", "extract.extract_users"}
    # 主链：transform -> build -> check -> publish
    assert {t.task_id for t in dag.get_task("build_mart").upstream_list} == {"transform"}
    assert {t.task_id for t in dag.get_task("quality_check").upstream_list} == {"build_mart"}
    assert {t.task_id for t in dag.get_task("publish_dataset").upstream_list} == {"quality_check"}


def test_retry_policy(dag_bag):
    for dag_id in EXPECTED_DAGS:
        dag = dag_bag.dags[dag_id]
        for task in dag.tasks:
            assert task.retries and task.retries >= 2, f"{dag_id}.{task.task_id} 未配置重试"
            assert task.retry_delay is not None, f"{dag_id}.{task.task_id} 未配置重试间隔"


def test_dataset_lineage(dag_bag):
    """ETL 发布的数据资产必须正好等于报表 DAG 消费的数据资产。"""
    etl = dag_bag.dags["datamart_daily_etl"]
    report = dag_bag.dags["datamart_daily_report"]
    etl_outlets = {o.uri for t in etl.tasks for o in (t.outlets or [])}
    assert etl_outlets == {"postgres://warehouse:5432/warehouse/dm/daily_city_sales"}
    consumer_uris = {d.uri for d in report.schedule}
    assert etl_outlets == consumer_uris


def test_failure_demo_chain(dag_bag):
    dag = dag_bag.dags["datamart_failure_demo"]
    notify = dag.get_task("notify_done")
    assert {t.task_id for t in notify.upstream_list} == {"always_fail"}
    assert notify.trigger_rule == "all_done"


def test_deadline_alerts(dag_bag):
    """主 ETL 与失败演示 DAG 都必须配置 Deadline 告警（超时/漏跑）。"""
    expected = {
        "datamart_daily_etl": "etl_run_timeout",
        "datamart_failure_demo": "demo_deadline",
        "datamart_incremental_etl": "incremental_run_timeout",
    }
    for dag_id, name in expected.items():
        dag = dag_bag.dags[dag_id]
        deadlines = dag.deadline if isinstance(dag.deadline, list) else [dag.deadline]
        assert deadlines and deadlines[0] is not None, f"{dag_id} 未配置 DeadlineAlert"
        assert deadlines[0].name == name


def test_incremental_dag_structure(dag_bag):
    dag = dag_bag.dags["datamart_incremental_etl"]
    # 并行抽取 -> 汇聚 merge
    merge_upstream = {t.task_id for t in dag.get_task("merge_to_dwh").upstream_list}
    assert merge_upstream == {"extract.extract_orders_incr", "extract.extract_users_incr"}
    # 水位线提交必须在质检之后（不丢数语义）
    assert {t.task_id for t in dag.get_task("commit_watermarks").upstream_list} == {"quality_check"}
    # staging 共享，禁止并发运行
    assert dag.max_active_runs == 1
    # 增量 DAG 与主 DAG 发布同一数据资产（报表联动）
    incr_outlets = {o.uri for t in dag.tasks for o in (t.outlets or [])}
    assert incr_outlets == {"postgres://warehouse:5432/warehouse/dm/daily_city_sales"}


def test_backfill_dag(dag_bag):
    dag = dag_bag.dags["datamart_backfill"]
    assert dag.schedule is None  # 仅手动触发
    # 补跑完成后必须刷新日报
    assert {t.task_id for t in dag.get_task("refresh_report").upstream_list} == {"backfill_range"}
