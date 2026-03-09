"""7주차 학습용 DAG: 관측성(로그/콜백/추적정보) 기본기를 익히는 파일.

학습 포인트:
- JSON 구조 로그로 사람이 읽기 쉬운 로그를 기계적으로도 분석 가능하게 남기는 방법
- on_failure_callback으로 실패 이벤트를 표준 형태로 기록하는 패턴
- trace_id 같은 실행 추적 키를 태스크 로그에 일관되게 남기는 습관
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {"owner": "study", "retries": 2, "retry_delay": timedelta(minutes=1)}


def _conf(context):
    dag_run = context.get("dag_run")
    return dag_run.conf if dag_run and dag_run.conf else {}


def on_failure_callback(context):
    payload = {
        "event": "task_failure",
        "dag_id": context["dag"].dag_id,
        "task_id": context["task_instance"].task_id,
        "run_id": context["run_id"],
        "try_number": context["task_instance"].try_number,
    }
    print("실패 알림 콜백:")
    print(json.dumps(payload, ensure_ascii=False))


def emit_log(**context):
    conf = _conf(context)
    trace_id = conf.get("trace_id", "trace-local-001")
    print(json.dumps({"step": "emit_log", "trace_id": trace_id, "status": "started"}, ensure_ascii=False))


def run_core_job(**context):
    conf = _conf(context)
    scenario = conf.get("scenario", "happy_path")

    if scenario == "force_fail":
        raise AirflowFailException("강제 예외 발생 (force_fail)")

    print(json.dumps({"step": "run_core_job", "status": "success"}, ensure_ascii=False))


with DAG(
    dag_id="study_week7_observability",
    start_date=datetime(2026, 3, 4),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["week7", "logging", "monitoring"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t_emit = PythonOperator(
        task_id="emit_log",
        python_callable=emit_log,
        execution_timeout=timedelta(minutes=3),
        on_failure_callback=on_failure_callback,
    )
    t_run = PythonOperator(
        task_id="run_core_job",
        python_callable=run_core_job,
        execution_timeout=timedelta(minutes=3),
        on_failure_callback=on_failure_callback,
    )

    start >> t_emit >> t_run >> end


