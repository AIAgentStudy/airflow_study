"""4주차 학습용 DAG: Variable/Connection/Trigger conf 설정 분리를 익히는 파일.

학습 포인트:
- 코드 하드코딩 대신 Variable, Connection, dag_run.conf 사용법
- 잘못된 scenario, 빈 변수값, 잘못된 conn_id 등 설정 오류의 fail-fast 처리
- retries가 적용되는 실패와 재시도로 해결 불가능한 설정 오류를 구분하는 방법
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {"owner": "study", "retries": 2, "retry_delay": timedelta(minutes=1)}
SUPPORTED_SCENARIOS = {
    "happy_path",
    "missing_variable",
    "bad_connection",
    "fallback_recovery",
    "invalid_scenario",
    "empty_variable",
    "empty_conn_id",
    "conn_without_host",
}


def _conf(context):
    dag_run = context.get("dag_run")
    return dag_run.conf if dag_run and dag_run.conf else {}


def _scenario(conf):
    scenario = conf.get("scenario", "happy_path")
    if scenario not in SUPPORTED_SCENARIOS:
        allowed = ", ".join(sorted(SUPPORTED_SCENARIOS))
        raise AirflowFailException(
            f"지원하지 않는 scenario 입니다: {scenario} | allowed={allowed}"
        )
    return scenario


def read_variable(**context):
    conf = _conf(context)
    scenario = _scenario(conf)

    if scenario == "missing_variable":
        value = Variable.get("missing_required_key")
    else:
        value = Variable.get("study_mode", default_var="local")

    if scenario == "empty_variable" and not str(value).strip():
        raise AirflowFailException("study_mode Variable 값이 비어 있습니다.")

    print(f"Variable 확인 완료 | study_mode={value}")


def read_connection(**context):
    conf = _conf(context)
    scenario = _scenario(conf)
    conn_id = conf.get("conn_id", "postgres_default")

    if scenario == "bad_connection":
        conn_id = "not_existing_connection"
    elif scenario == "empty_conn_id":
        conn_id = ""

    if not str(conn_id).strip():
        raise AirflowFailException(
            "conn_id 값이 비어 있습니다. dag_run.conf 에 conn_id를 지정하세요."
        )

    conn = BaseHook.get_connection(conn_id)

    if scenario == "conn_without_host" and not (conn.host or "").strip():
        raise AirflowFailException(
            f"연결은 조회되었지만 host 값이 비어 있습니다. conn_id={conn.conn_id}"
        )

    print(f"Connection 확인 완료 | conn_id={conn.conn_id} | host={conn.host}")


def fallback_recovery(**context):
    conf = _conf(context)
    scenario = _scenario(conf)

    if scenario == "fallback_recovery":
        value = Variable.get("missing_required_key", default_var="fallback-enabled")
        print(f"기본값 복구 적용 | value={value}")
    elif scenario == "invalid_scenario":
        raise AirflowFailException("의도적 실패 시나리오: invalid_scenario")
    else:
        print("기본 경로 실행")


with DAG(
    dag_id="study_week4_config",
    start_date=datetime(2026, 3, 4),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["week4", "variables", "connections"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t_var = PythonOperator(
        task_id="read_variable",
        python_callable=read_variable,
        execution_timeout=timedelta(minutes=3),
    )
    t_conn = PythonOperator(
        task_id="read_connection",
        python_callable=read_connection,
        execution_timeout=timedelta(minutes=3),
    )
    t_fallback = PythonOperator(
        task_id="fallback_recovery",
        python_callable=fallback_recovery,
        execution_timeout=timedelta(minutes=3),
    )

    start >> t_var >> t_conn >> t_fallback >> end
