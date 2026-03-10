"""11주차 학습용 DAG: 운영(Ops) 관점의 보안/권한/변경관리 점검을 익히는 파일.

학습 포인트:
- Variable 기반 시크릿 확인과 누락 시 실패 처리
- Connection 권한 문제(permission_denied) 재현으로 접근 제어 점검
- 백업/롤백 계획과 업그레이드 리허설을 태스크로 명시하는 운영 습관
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


DEFAULT_ARGS = {"owner": "study", "retries": 1, "retry_delay": timedelta(minutes=1)}


def _conf(context):
    dag_run = context.get("dag_run")
    return dag_run.conf if dag_run and dag_run.conf else {}


def check_secret(**context):
    conf = _conf(context)
    scenario = conf.get("scenario", "happy_path")

    if scenario == "secret_missing":
        _ = Variable.get("required_secret_key")
    else:
        _ = Variable.get("required_secret_key", default_var="dummy-local-secret")

    print("비밀정보 확인 완료")


def check_connection_permission(**context):
    conf = _conf(context)
    conn_id = conf.get("conn_id", "postgres_default")
    conn = BaseHook.get_connection(conn_id)

    if conf.get("scenario") == "permission_denied":
        raise AirflowFailException(f"권한 부족 재현 | conn_id={conn.conn_id}")

    print(f"권한 체크 통과 | conn_id={conn.conn_id}")


def backup_and_rollback_plan():
    print("백업/롤백 점검표 확인 완료")


def upgrade_dry_run(**context):
    conf = _conf(context)
    if conf.get("scenario") == "upgrade_risk":
        raise AirflowFailException("업그레이드 리허설 실패 재현 (upgrade_risk)")
    print("업그레이드 리허설 통과")


with DAG(
    dag_id="study_week11_ops",
    start_date=datetime(2026, 3, 4),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["week11", "ops", "security"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t_secret = PythonOperator(task_id="check_secret", python_callable=check_secret, execution_timeout=timedelta(minutes=3))
    t_perm = PythonOperator(task_id="check_connection_permission", python_callable=check_connection_permission, execution_timeout=timedelta(minutes=3))
    t_plan = PythonOperator(task_id="backup_and_rollback_plan", python_callable=backup_and_rollback_plan, execution_timeout=timedelta(minutes=3))
    t_upgrade = PythonOperator(task_id="upgrade_dry_run", python_callable=upgrade_dry_run, execution_timeout=timedelta(minutes=3))

    start >> t_secret >> t_perm >> t_plan >> t_upgrade >> end


