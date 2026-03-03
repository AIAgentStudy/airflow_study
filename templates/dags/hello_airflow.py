"""Airflow 기본 동작을 익히기 위한 가장 단순한 DAG 예제.

두 개의 Bash 태스크를 순차 실행하며 스케줄, 태스크 의존성, 실행 로그 확인을 연습한다.
"""

from datetime import datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

with DAG(
    dag_id="study_hello_airflow",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["study", "week1"],
) as dag:
    t1 = BashOperator(task_id="print_date", bash_command="date")
    t2 = BashOperator(task_id="say_hello", bash_command="echo 안녕하세요_에어플로우")

    t1 >> t2
