"""10주차 학습용 DAG: CI 품질 게이트 흐름을 Airflow로 모델링한 파일.

학습 포인트:
- lint -> unit test -> integration gate -> release_ready 순차 게이트 설계
- 실패 지점을 scenario로 주입해 파이프라인 차단 원리 이해
- 배포 전 자동 검증 통과 기준을 워크플로우로 명시하는 연습
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {"owner": "study", "retries": 1, "retry_delay": timedelta(minutes=1)}


def _conf(context):
    dag_run = context.get("dag_run")
    return dag_run.conf if dag_run and dag_run.conf else {}


def lint_check(**context):
    conf = _conf(context)
    if conf.get("scenario") == "lint_fail":
        raise AirflowFailException("lint 실패 재현 (lint_fail)")
    print("lint 체크 통과")


def unit_test_check(**context):
    conf = _conf(context)
    if conf.get("scenario") == "test_fail":
        raise AirflowFailException("unit test 실패 재현 (test_fail)")
    print("unit test 통과")


def integration_gate(**context):
    conf = _conf(context)
    if conf.get("scenario") == "integration_fail":
        raise AirflowFailException("integration gate 실패 재현")
    print("integration gate 통과")


def release_ready():
    print("배포 가능 상태 확인 완료")


with DAG(
    dag_id="study_week10_ci_ready",
    start_date=datetime(2026, 3, 4),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["week10", "ci", "quality-gate"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t_lint = PythonOperator(task_id="lint_check", python_callable=lint_check, execution_timeout=timedelta(minutes=3))
    t_unit = PythonOperator(task_id="unit_test_check", python_callable=unit_test_check, execution_timeout=timedelta(minutes=3))
    t_integration = PythonOperator(task_id="integration_gate", python_callable=integration_gate, execution_timeout=timedelta(minutes=3))
    t_release = PythonOperator(task_id="release_ready", python_callable=release_ready, execution_timeout=timedelta(minutes=3))

    start >> t_lint >> t_unit >> t_integration >> t_release >> end
