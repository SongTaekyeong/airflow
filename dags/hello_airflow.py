from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2025, 1, 1),
    schedule=None,  # 수동 실행
    catchup=False,
) as dag:
    t1 = BashOperator(
        task_id="say_hello",
        bash_command="echo 'hello airflow' && date",
    )
