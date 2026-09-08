"""
DataMart 每日汇总报表 DAG：由上游 ETL 的 Dataset 自动触发（数据血缘驱动，无需定时）。

触发条件：datamart_daily_etl 的 publish_dataset 任务成功发布
Asset(uri="postgres://warehouse/warehouse/dm/daily_city_sales") 后自动运行。

在 UI 的 Datasets 页面可查看血缘图：datamart_daily_etl -> 数据集 -> 本 DAG。
"""

import logging
from datetime import timedelta

import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, Asset

log = logging.getLogger(__name__)

CONN_ID = "warehouse"
DS_DAILY_CITY_SALES = Asset(uri="postgres://warehouse/warehouse/dm/daily_city_sales")

default_args = {
    "owner": "datamart",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def refresh_report():
    """汇总最新一期城市销售数据，生成全局日报表（幂等）。"""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    hook.run(
        """
        DELETE FROM dm.daily_summary_report
        WHERE stat_date = (SELECT max(stat_date) FROM dm.daily_city_sales)
        """
    )
    hook.run(
        """
        INSERT INTO dm.daily_summary_report (stat_date, city_cnt, order_cnt, total_amount)
        SELECT stat_date, count(*), sum(order_cnt), sum(total_amount)
        FROM dm.daily_city_sales
        WHERE stat_date = (SELECT max(stat_date) FROM dm.daily_city_sales)
        GROUP BY stat_date
        """
    )
    row = hook.get_first(
        "SELECT stat_date, city_cnt, order_cnt, total_amount FROM dm.daily_summary_report "
        "ORDER BY stat_date DESC LIMIT 1"
    )
    log.info("日报生成完成: stat_date=%s, 城市数=%s, 订单数=%s, 总金额=%s", *row)


with DAG(
    dag_id="datamart_daily_report",
    description="DataMart 日报：由 dm.daily_city_sales 数据集变更自动触发",
    schedule=[DS_DAILY_CITY_SALES],  # Dataset 驱动，而非定时
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Shanghai"),
    catchup=False,
    default_args=default_args,
    tags=["datamart", "report"],
) as dag:
    PythonOperator(task_id="refresh_summary_report", python_callable=refresh_report)
