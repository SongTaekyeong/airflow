from __future__ import annotations

from datetime import datetime
import os
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator


DATA_PATH = "/opt/airflow/data/orders.csv"
OUTPUT_DIR = "/opt/airflow/output"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "kpi_summary.csv")


def extract_orders(**context):
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError(f"CSV not found: {DATA_PATH}")

    df = pd.read_csv(DATA_PATH)
    # 간단한 타입 보정
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    context["ti"].xcom_push(key="orders_rows", value=len(df))
    # XCom에 큰 데이터를 직접 넣지 않기 (실무 습관) -> 파일 기반으로 이어감
    df.to_parquet(os.path.join(OUTPUT_DIR, "orders.parquet"), index=False)


def calculate_kpi(**context):
    parquet_path = os.path.join(OUTPUT_DIR, "orders.parquet")
    if not os.path.exists(parquet_path):
        raise FileNotFoundError("orders.parquet missing. extract_orders ran?")

    df = pd.read_parquet(parquet_path)

    # 일자별 KPI (전체 기준)
    grp = df.groupby("order_date", as_index=False).agg(
        total_orders=("order_id", "count"),
        delayed_orders=("is_delayed", "sum"),
        cold_damage_orders=("is_cold_damage", "sum"),
    )
    grp["delay_rate"] = (grp["delayed_orders"] / grp["total_orders"]).round(4)
    grp["cold_damage_rate"] = (grp["cold_damage_orders"] / grp["total_orders"]).round(4)

    grp.to_csv(OUTPUT_PATH, index=False)
    context["ti"].xcom_push(key="kpi_path", value=OUTPUT_PATH)


def done_message(**context):
    rows = context["ti"].xcom_pull(task_ids="extract_orders", key="orders_rows")
    kpi_path = context["ti"].xcom_pull(task_ids="calculate_kpi", key="kpi_path")
    print(f"[DONE] processed rows={rows}, saved KPI to {kpi_path}")


with DAG(
    dag_id="logistics_kpi_daily",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # 수동 실행 (처음엔 이게 베스트)
    catchup=False,
    tags=["mini-project", "kpi", "logistics"],
) as dag:

    t1 = PythonOperator(
        task_id="extract_orders",
        python_callable=extract_orders,
    )

    t2 = PythonOperator(
        task_id="calculate_kpi",
        python_callable=calculate_kpi,
    )

    t3 = PythonOperator(
        task_id="done_message",
        python_callable=done_message,
    )

    t1 >> t2 >> t3
