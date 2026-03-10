"""9주차 학습용 DAG: DAG 테스트 관점을 태스크 흐름으로 익히는 파일.

학습 포인트:
- import 검사로 배포 전 기본 무결성 확인(DagBag import 에러 예방)
- 구조 검증(예상 태스크 수)처럼 기준 기반 체크를 자동화하는 방법
- 단위 테스트 실패 시나리오를 재현해 실패 로그와 복구 절차 연습
"""

import importlib
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {"owner": "study", "retries": 1, "retry_delay": timedelta(minutes=1)}


def _conf(context):
    dag_run = context.get("dag_run")
    return dag_run.conf if dag_run and dag_run.conf else {}


def dagbag_import_check(**context):
    conf = _conf(context)
    module_name = conf.get("module_name", "json")
    if conf.get("scenario") == "bad_import":
        module_name = "not_existing_module_abc"

    importlib.import_module(module_name)
    print(f"import 체크 성공 | module={module_name}")


def structure_check(**context):
    conf = _conf(context)
    expected_tasks = int(conf.get("expected_tasks", 3))
    actual_tasks = int(conf.get("actual_tasks", 3))
    if actual_tasks < expected_tasks:
        raise AirflowFailException(f"구조 검증 실패: actual={actual_tasks}, expected={expected_tasks}")
    print("구조 검증 성공")


def unit_function_check(**context):
    conf = _conf(context)
    if conf.get("scenario") == "unit_test_fail":
        raise AirflowFailException("단위 테스트 실패 재현 (unit_test_fail)")
    print("단위 테스트 성공")


with DAG(
    dag_id="study_week9_testing",
    start_date=datetime(2026, 3, 4),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["week9", "testing"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t_import = PythonOperator(task_id="dagbag_import_check", python_callable=dagbag_import_check, execution_timeout=timedelta(minutes=3))
    t_structure = PythonOperator(task_id="structure_check", python_callable=structure_check, execution_timeout=timedelta(minutes=3))
    t_unit = PythonOperator(task_id="unit_function_check", python_callable=unit_function_check, execution_timeout=timedelta(minutes=3))

    start >> t_import >> t_structure >> t_unit >> end


