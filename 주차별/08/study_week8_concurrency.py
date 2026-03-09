"""8주차 학습용 DAG: 동시성(concurrency)과 자원 병목(pool) 개념을 익히는 파일.

학습 포인트:
- max_active_runs, max_active_tasks 설정이 실행 속도/대기에 미치는 영향
- 여러 worker 태스크를 병렬 실행할 때의 상태 변화 관찰
- configured_slots < required_slots 상황을 재현해 pool 병목 원인 분석
"""

import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {"owner": "study", "retries": 1, "retry_delay": timedelta(minutes=1)}


def _conf(context):
    dag_run = context.get("dag_run")
    return dag_run.conf if dag_run and dag_run.conf else {}


def check_pool_settings(**context):
    conf = _conf(context)
    scenario = conf.get("scenario", "happy_path")
    configured_slots = int(conf.get("configured_slots", 2))
    required_slots = int(conf.get("required_slots", 2))

    if scenario == "pool_bottleneck" and configured_slots < required_slots:
        raise AirflowFailException(
            f"Pool 병목: configured_slots={configured_slots} < required_slots={required_slots}"
        )

    print(f"Pool 설정 확인 완료 | configured={configured_slots}, required={required_slots}")


def run_parallel_unit(unit_name: str, sleep_seconds: int):
    print(f"작업 시작 | unit={unit_name}")
    time.sleep(sleep_seconds)
    print(f"작업 완료 | unit={unit_name}")


with DAG(
    dag_id="study_week8_concurrency",
    start_date=datetime(2026, 3, 4),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=4,
    tags=["week8", "concurrency", "pool"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t_check = PythonOperator(task_id="check_pool_settings", python_callable=check_pool_settings)

    workers = []
    for idx in range(1, 7):
        workers.append(
            PythonOperator(
                task_id=f"worker_{idx}",
                python_callable=run_parallel_unit,
                op_kwargs={"unit_name": f"w{idx}", "sleep_seconds": 3},
                execution_timeout=timedelta(minutes=3),
            )
        )

    start >> t_check >> workers >> end


