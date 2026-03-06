"""1주차 학습용 DAG: Airflow의 가장 기본 구조를 익히기 위한 파일.

학습 포인트:
- DAG/Task 기본 뼈대(start -> 작업 -> end) 작성법
- retries, retry_delay, execution_timeout 같은 안정성 옵션 의미
- Trigger conf로 의도적 실패(command_fail)를 재현하고 로그에서 원인 찾기
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {"owner": "study", "retries": 2, "retry_delay": timedelta(minutes=1)}


def _conf(context):
    dag_run = context.get("dag_run")
    return dag_run.conf if dag_run and dag_run.conf else {}


def say_hello():
    print("안녕하세요, Airflow 1주차 DAG입니다.")


def simulate_failure(**context):
    conf = _conf(context)
    scenario = conf.get("scenario", "happy_path")

    if scenario == "command_fail":
        raise AirflowFailException("의도적 실패 재현 (command_fail)")

    print("실패 주입 태스크 통과")


def finish():
    print("1주차 기본 DAG 완료")


with DAG(
    dag_id="study_week1_foundation",
    start_date=datetime(2026, 3, 4),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["week1", "foundation"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t_hello = PythonOperator(task_id="say_hello", python_callable=say_hello, execution_timeout=timedelta(minutes=3))
    t_fail = PythonOperator(task_id="simulate_failure", python_callable=simulate_failure, execution_timeout=timedelta(minutes=3))
    t_finish = PythonOperator(task_id="finish", python_callable=finish, execution_timeout=timedelta(minutes=3))

    start >> t_hello >> t_fail >> t_finish >> end


