"""12주차 학습용 DAG: 캡스톤 형태의 end-to-end 파이프라인을 익히는 파일.

학습 포인트:
- 수집 -> 변환 -> 검증 -> 알림 -> 종료의 전체 데이터 파이프라인 설계
- 품질 기준(min_rows) 미달 시 실패 처리와 로그 기반 원인 파악
- TriggerRule.ALL_DONE을 사용해 실패 상황에서도 알림 태스크를 실행하는 패턴
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule


DEFAULT_ARGS = {"owner": "study", "retries": 2, "retry_delay": timedelta(minutes=1)}


def _conf(context):
    dag_run = context.get("dag_run")
    return dag_run.conf if dag_run and dag_run.conf else {}


def collect_source(**context):
    conf = _conf(context)
    if conf.get("scenario") == "source_fail":
        raise AirflowFailException("수집 단계 실패 재현 (source_fail)")
    print("수집 완료")


def transform_data(**context):
    conf = _conf(context)
    rows = int(conf.get("rows", 100))
    print(f"변환 완료 | rows={rows}")


def validate_data(**context):
    conf = _conf(context)
    min_rows = int(conf.get("min_rows", 10))
    rows = int(conf.get("rows", 100))

    if conf.get("scenario") == "quality_fail":
        rows = min_rows - 1

    if rows < min_rows:
        raise AirflowFailException(f"검증 실패 | rows={rows}, min_rows={min_rows}")

    print("검증 통과")


def notify_result(**context):
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else "unknown"
    print(f"알림 전송 완료 | run_id={run_id}")


def finalize():
    print("캡스톤 파이프라인 종료")


with DAG(
    dag_id="study_week12_capstone",
    start_date=datetime(2026, 3, 4),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["week12", "capstone", "end-to-end"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t_collect = PythonOperator(task_id="collect_source", python_callable=collect_source, execution_timeout=timedelta(minutes=3))
    t_transform = PythonOperator(task_id="transform_data", python_callable=transform_data, execution_timeout=timedelta(minutes=3))
    t_validate = PythonOperator(task_id="validate_data", python_callable=validate_data, execution_timeout=timedelta(minutes=3))
    t_notify = PythonOperator(
        task_id="notify_result",
        python_callable=notify_result,
        execution_timeout=timedelta(minutes=3),
        trigger_rule=TriggerRule.ALL_DONE,
    )
    t_finalize = PythonOperator(task_id="finalize", python_callable=finalize, execution_timeout=timedelta(minutes=3))

    start >> t_collect >> t_transform >> t_validate >> t_notify >> t_finalize >> end


