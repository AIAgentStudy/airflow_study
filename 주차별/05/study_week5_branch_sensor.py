"""5주차 학습용 DAG: Sensor + Branch + TriggerRule 비교를 실전 시나리오로 익히는 파일.

이 DAG는 "대기(sensor) -> 분기(branch) -> 합류(join)" 구조에서
실패/스킵/오분기/타임아웃이 어떻게 전파되는지 학습하기 위한 예제입니다.

사용 방법(Trigger DAG -> JSON conf):
- 기본 성공: {"scenario":"happy_path","route":"fast"}
- 센서 타임아웃: {"scenario":"sensor_timeout"}
- 센서 예외: {"scenario":"sensor_exception"}
- 오분기(없는 task 반환): {"scenario":"wrong_branch"}
- 잘못된 route 값: {"scenario":"invalid_route"}
- fast 분기 실패: {"scenario":"fast_fail","route":"fast"}
- safe 분기 실패: {"scenario":"safe_fail","route":"safe"}
- fast 분기 스킵: {"scenario":"fast_skip","route":"fast"}
- safe 분기 스킵: {"scenario":"safe_skip","route":"safe"}

관찰 포인트:
1) join_lenient(NONE_FAILED_MIN_ONE_SUCCESS): 한 분기만 성공해도 합류 가능
2) join_strict(ALL_SUCCESS): 모든 upstream 성공일 때만 합류 가능
3) skip/failed/upstream_failed 상태가 TriggerRule에 따라 어떻게 달라지는지 비교
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.trigger_rule import TriggerRule


DEFAULT_ARGS = {"owner": "study", "retries": 2, "retry_delay": timedelta(minutes=1)}
SUPPORTED_SCENARIOS = {
    "happy_path",
    "sensor_timeout",
    "sensor_exception",
    "wrong_branch",
    "invalid_route",
    "fast_fail",
    "safe_fail",
    "fast_skip",
    "safe_skip",
}
SUPPORTED_ROUTES = {"fast", "safe"}


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


def sensor_check(**context):
    """센서 단계 시나리오.

    시나리오:
    - sensor_timeout: 파일 존재 여부와 무관하게 False를 반환해 timeout 유도
    - sensor_exception: 센서 내부 예외를 강제로 발생시켜 즉시 실패
    - happy_path 및 기타: watch_path 파일 존재 여부를 실제 확인
    """
    conf = _conf(context)
    scenario = _scenario(conf)
    path = conf.get("watch_path", "/opt/airflow/dags/data/ready.flag")

    if scenario == "sensor_exception":
        raise AirflowFailException("sensor 단계 의도적 예외 (sensor_exception)")
    if scenario == "sensor_timeout":
        print("센서 타임아웃 시나리오: 항상 False 반환")
        return False

    exists = os.path.exists(path)
    print(f"센서 체크 | path={path} | exists={exists}")
    return exists


def choose_branch(**context):
    """분기 단계 시나리오.

    시나리오:
    - wrong_branch: 존재하지 않는 task_id를 반환해 분기 실패 재현
    - invalid_route: 허용되지 않은 route를 강제로 넣어 입력 검증 실패 재현

    일반 동작:
    - route=fast -> fast_path
    - route=safe -> safe_path
    """
    conf = _conf(context)
    scenario = _scenario(conf)

    if scenario == "wrong_branch":
        return "not_existing_task"

    route = conf.get("route", "fast")
    if scenario == "invalid_route":
        route = "unknown"

    if route not in SUPPORTED_ROUTES:
        allowed = ", ".join(sorted(SUPPORTED_ROUTES))
        raise AirflowFailException(
            f"지원하지 않는 route 입니다: {route} | allowed={allowed}"
        )

    return "fast_path" if route == "fast" else "safe_path"


def run_fast(**context):
    """fast 경로 시나리오.

    - fast_fail: fast 경로에서 실패
    - fast_skip: fast 경로에서 스킵
    """
    scenario = _scenario(_conf(context))
    if scenario == "fast_fail":
        raise AirflowFailException("fast 경로 의도적 실패 (fast_fail)")
    if scenario == "fast_skip":
        raise AirflowSkipException("fast 경로 의도적 스킵 (fast_skip)")
    print("fast 경로 실행")


def run_safe(**context):
    """safe 경로 시나리오.

    - safe_fail: safe 경로에서 실패
    - safe_skip: safe 경로에서 스킵
    """
    scenario = _scenario(_conf(context))
    if scenario == "safe_fail":
        raise AirflowFailException("safe 경로 의도적 실패 (safe_fail)")
    if scenario == "safe_skip":
        raise AirflowSkipException("safe 경로 의도적 스킵 (safe_skip)")
    print("safe 경로 실행")


def summarize_rules():
    """TriggerRule 비교용 안내 로그를 남깁니다.

    join_lenient: NONE_FAILED_MIN_ONE_SUCCESS
    join_strict: ALL_SUCCESS
    """
    print(
        "TriggerRule 비교 완료 | join_lenient=NONE_FAILED_MIN_ONE_SUCCESS | join_strict=ALL_SUCCESS"
    )


with DAG(
    dag_id="study_week5_branch_sensor",
    start_date=datetime(2026, 3, 4),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["week5", "sensor", "branch", "trigger-rule"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    wait_ready = PythonSensor(
        task_id="wait_ready",
        python_callable=sensor_check,
        poke_interval=5,
        timeout=20,
        mode="reschedule",
    )

    branch = BranchPythonOperator(
        task_id="branch_route",
        python_callable=choose_branch,
        execution_timeout=timedelta(minutes=3),
    )

    fast_path = PythonOperator(
        task_id="fast_path",
        python_callable=run_fast,
        execution_timeout=timedelta(minutes=3),
    )
    safe_path = PythonOperator(
        task_id="safe_path",
        python_callable=run_safe,
        execution_timeout=timedelta(minutes=3),
    )

    # lenient: 한 분기만 성공해도 join 실행 가능
    join_lenient = EmptyOperator(
        task_id="join_lenient",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # strict: 모든 upstream 성공이어야 join 실행
    join_strict = EmptyOperator(
        task_id="join_strict",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    summarize = PythonOperator(
        task_id="summarize_rules",
        python_callable=summarize_rules,
        execution_timeout=timedelta(minutes=3),
        # 비교 결과를 항상 남기기 위해 upstream 상태와 무관하게 실행
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start >> wait_ready >> branch
    branch >> [fast_path, safe_path]
    [fast_path, safe_path] >> join_lenient >> summarize
    [fast_path, safe_path] >> join_strict >> summarize
    summarize >> end
