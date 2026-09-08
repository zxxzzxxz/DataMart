"""
DataMart 每日 ETL 主 DAG：ods 源层 -> dwh 明细层 -> dm 集市层

真实数据流水线（连接 conn_id="warehouse"，库内分 ods/dwh/dm 三个 schema）：
- 串行依赖：extract -> transform -> build -> check -> publish
- 并行分支：orders / users 两条抽取线同时跑（TaskGroup）
- 汇聚依赖：transform 等待两条抽取线全部完成
- 重试机制：统一 default_args，失败自动重试（指数退避）
- 幂等设计：transform 用 ON CONFLICT DO NOTHING，重复执行/补跑不产生重复数据
- XCom 数据传递：抽取行数在任务间流转，供质检对账
- 质量校验：源层与明细层行数对账，不一致则失败触发重试与告警
- 失败告警：on_failure_callback -> plugins/alerting.py（支持 webhook）
- 数据血缘：成功后发布 Dataset，自动触发下游 datamart_daily_report
"""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset, get_current_context, task_group
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference, SyncCallback
from alerting import send_failure_alert

log = logging.getLogger(__name__)

CONN_ID = "warehouse"
# 数据资产标识：下游 DAG 通过它建立血缘依赖（UI 的 Datasets 页可见）
DS_DAILY_CITY_SALES = Asset(uri="postgres://warehouse/warehouse/dm/daily_city_sales")

# ---------- 任务默认参数：重试 / 告警 ----------
default_args = {
    "owner": "datamart",
    "retries": 3,  # 失败后最多重试 3 次
    "retry_delay": timedelta(minutes=1),  # 首次重试间隔 1 分钟
    "retry_exponential_backoff": True,  # 之后按指数退避：1m -> 2m -> 4m
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": send_failure_alert,  # 重试耗尽后触发告警
    # 注意：SLA 已在 Airflow 3.0 移除，超时告警请使用 Deadline Alerts
    # "email_on_failure": True,                    # 配好 SMTP 后打开即可邮件告警
}


def _stat_date() -> str:
    """本次运行的统计日期。

    Airflow 3 中手动触发的运行 logical_date 为空（仅调度运行才有），
    因此做兜底：无 logical_date 时使用当前业务日期（东八区）。
    """
    logical_date = get_current_context().get("logical_date")
    if logical_date is not None:
        return logical_date.strftime("%Y-%m-%d")
    return pendulum.now("Asia/Shanghai").strftime("%Y-%m-%d")


def extract(table: str):
    """从 ODS 源层抽取并校验，行数通过 XCom 传给下游质检对账。"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    cnt = hook.get_first(f"SELECT count(*) FROM ods.{table}")[0]
    log.info("ODS 抽取完成: ods.%s 共 %d 行", table, cnt)
    return cnt


def transform():
    """明细层加工：orders 关联 users 打宽，幂等写入 dwh.fact_orders。"""
    ds = _stat_date()
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(
        """
        INSERT INTO dwh.fact_orders (order_id, user_id, user_name, city, amount, status, order_date, etl_date)
        SELECT o.order_id, o.user_id, u.user_name, u.city, o.amount, o.status, o.order_date, %(ds)s::date
        FROM ods.orders o
        JOIN ods.users u ON u.user_id = o.user_id
        ON CONFLICT (order_id) DO NOTHING
        """,
        parameters={"ds": ds},
    )
    cnt = hook.get_first("SELECT count(*) FROM dwh.fact_orders")[0]
    log.info("DWH 加工完成: dwh.fact_orders 共 %d 行", cnt)


def build_mart():
    """集市层汇总：按 业务日期+城市 聚合已支付订单（全量刷新，幂等）。"""
    run_date = _stat_date()
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run("TRUNCATE TABLE dm.daily_city_sales")
    hook.run(
        """
        INSERT INTO dm.daily_city_sales (stat_date, city, order_cnt, total_amount)
        SELECT order_date, city, count(*), sum(amount)
        FROM dwh.fact_orders
        WHERE status = 'paid'
        GROUP BY order_date, city
        """,
    )
    log.info("DM 汇总完成: dm.daily_city_sales 全量刷新（运行日期 %s）", run_date)


def quality_check():
    """质量对账（全量语义）：ods 源层行数 == dwh 明细层行数，且 dm 集市非空。"""
    ti = get_current_context()["ti"]
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    src = hook.get_first("SELECT count(*) FROM ods.orders")[0]
    dwh = hook.get_first("SELECT count(*) FROM dwh.fact_orders")[0]
    dm = hook.get_first("SELECT count(*) FROM dm.daily_city_sales")[0]
    extract_cnt = ti.xcom_pull(task_ids="extract.extract_orders")
    log.info("对账: ods=%d, dwh=%d, dm=%d, extract_xcom=%s", src, dwh, dm, extract_cnt)
    if src != dwh:
        raise ValueError(f"行数对不上：ods={src} vs dwh={dwh}")
    if extract_cnt is not None and int(extract_cnt) != src:
        raise ValueError(f"抽取行数与源库不一致：xcom={extract_cnt} vs ods={src}")
    if dm == 0:
        raise ValueError("dm.daily_city_sales 无数据")
    log.info("质量校验通过")


with DAG(
    dag_id="datamart_daily_etl",
    description="DataMart 每日 ETL：ods -> dwh -> dm，含对账质检与 Dataset 血缘发布",
    schedule="@daily",  # 调度周期：每天一次
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,  # 不补跑历史周期
    default_args=default_args,
    tags=["datamart", "etl"],
    # Deadline 告警：Run 入队后 1 小时内未完成即告警（超时/漏跑），由 alert-webhook 通道接收
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(hours=1),
        callback=SyncCallback("alerting.send_deadline_alert"),
        name="etl_run_timeout",
    ),
) as dag:
    start = EmptyOperator(task_id="start")

    # 并行抽取两支（TaskGroup 在 Graph 视图中折叠为一个分组）
    @task_group(group_id="extract")
    def extract_group():
        PythonOperator(
            task_id="extract_orders",
            python_callable=extract,
            op_kwargs={"table": "orders"},
        )
        PythonOperator(
            task_id="extract_users",
            python_callable=extract,
            op_kwargs={"table": "users"},
        )

    transform_task = PythonOperator(task_id="transform", python_callable=transform)

    build = PythonOperator(task_id="build_mart", python_callable=build_mart)

    check = PythonOperator(task_id="quality_check", python_callable=quality_check)

    # 发布数据资产：成功后触发下游 datamart_daily_report
    publish = EmptyOperator(task_id="publish_dataset", outlets=[DS_DAILY_CITY_SALES])

    # 无论成败都执行的通知节点（演示 trigger_rule=all_done）
    notify = EmptyOperator(task_id="notify_done", trigger_rule="all_done")

    end = EmptyOperator(task_id="end")

    # ---------- 依赖拓扑 ----------
    # start -> [并行抽取] -> 打宽 -> 集市汇总 -> 质检 -> 发布资产 -> 结束
    start >> extract_group() >> transform_task >> build >> check >> publish >> end
    check >> notify
