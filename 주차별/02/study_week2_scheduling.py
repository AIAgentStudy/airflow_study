"""2주차 학습용 DAG: 스케줄링과 실행 제어를 실습하는 파일.

학습 포인트:
- scenario/scenarios conf로 다양한 실패/스킵/타임아웃 상황 주입
- TriggerRule로 분기/조인 시 downstream 상태 전파 이해
- 파일 누락, 품질 저하, 지연 처리 같은 운영성 이슈 재현과 복구 연습
"""

import os
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "study",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


def _log(msg: str) -> None:
    print(msg)


def _get_scenarios(context) -> set[str]:
    """DAG Run conf에서 시나리오 목록을 읽어 set으로 반환합니다."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    scenarios = conf.get("scenarios")
    single_scenario = conf.get("scenario")

    if isinstance(scenarios, list):
        return {str(s).strip() for s in scenarios if str(s).strip()}
    if isinstance(single_scenario, str) and single_scenario.strip():
        return {single_scenario.strip()}
    return set()


def prepare(**context) -> None:
    scenarios = sorted(_get_scenarios(context))
    _log(f"준비 단계 완료 | scenarios={scenarios if scenarios else ['happy_path']}")


def branch_a() -> None:
    _log("branch_a 성공")


def branch_b(**context) -> None:
    """분기 실패 시나리오를 재현합니다.

    conf 예시:
    {"scenario": "branch_fail"}
    {"scenarios": ["branch_fail", "data_quality_fail"]}
    """
    scenarios = _get_scenarios(context)
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    if "branch_fail" in scenarios or bool(conf.get("fail_branch_b", False)):
        raise AirflowFailException("branch_b 의도적 실패 (branch_fail)")

    _log("branch_b 성공")


def check_input_file(**context) -> None:
    """외부 입력 파일 누락 시나리오를 재현합니다."""
    scenarios = _get_scenarios(context)
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    required = "missing_input" in scenarios or bool(
        conf.get("require_input_file", False)
    )
    input_path = conf.get("input_path", "/opt/airflow/dags/data/input.csv")

    if required and not os.path.exists(input_path):
        raise AirflowFailException(f"입력 파일 누락: {input_path}")

    _log(f"입력 파일 체크 통과 | required={required} | path={input_path}")


def validate_quality(**context) -> None:
    """데이터 품질 실패 시나리오를 재현합니다."""
    scenarios = _get_scenarios(context)
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    min_rows = int(conf.get("min_rows", 10))
    row_count = int(conf.get("row_count", 100))

    if "data_quality_fail" in scenarios and "row_count" not in conf:
        row_count = min_rows - 1

    if row_count < min_rows:
        raise AirflowFailException(
            f"데이터 품질 실패: row_count={row_count}, min_rows={min_rows}"
        )

    _log(f"데이터 품질 통과 | row_count={row_count}, min_rows={min_rows}")


def simulate_processing(**context) -> None:
    """타임아웃 시나리오를 재현합니다."""
    scenarios = _get_scenarios(context)
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run and dag_run.conf else {}

    sleep_seconds = int(conf.get("sleep_seconds", 1))
    if "timeout" in scenarios and "sleep_seconds" not in conf:
        sleep_seconds = 10

    _log(f"처리 태스크 시작 | sleep_seconds={sleep_seconds}")
    time.sleep(sleep_seconds)
    _log("처리 태스크 완료")


def optional_path(**context) -> None:
    """스킵 상태 전파 관찰용 시나리오입니다."""
    scenarios = _get_scenarios(context)
    if "skip_optional" in scenarios:
        raise AirflowSkipException("optional_path 의도적 스킵 (skip_optional)")

    _log("optional_path 실행 완료")


def summarize_result(**context) -> None:
    """실행 결과 요약 로그를 출력합니다.

    2주차 학습 목적상 기본값은 '요약 태스크 자체는 실패하지 않음'입니다.
    """
    dag_run = context.get("dag_run")
    ti = context.get("task_instance")
    run_id = dag_run.run_id if dag_run else "unknown"

    try:
        tis = dag_run.get_task_instances() if dag_run else []
        states = {t.task_id: str(t.state) for t in tis}
        _log(f"실행 결과 요약 | run_id={run_id} | states={states}")
    except Exception as exc:
        # 요약 조회 오류는 로그로만 남기고 태스크는 성공 처리해 학습 흐름을 유지한다.
        _log(f"요약 조회 경고(run_id={run_id}): {exc}")
        _log(
            f"fallback 요약 | task_id={ti.task_id if ti else 'unknown'} "
            f"| try_number={ti.try_number if ti else 'unknown'}"
        )


with DAG(
    dag_id="study_week2_scheduling",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 3, 4),
    schedule="@daily",
    catchup=False,
    tags=["week2", "scheduling", "failure-scenarios"],
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    t_prepare = PythonOperator(
        task_id="prepare",
        python_callable=prepare,
        execution_timeout=timedelta(minutes=3),
    )
    t_branch_a = PythonOperator(
        task_id="branch_a",
        python_callable=branch_a,
        execution_timeout=timedelta(minutes=3),
    )
    t_branch_b = PythonOperator(
        task_id="branch_b",
        python_callable=branch_b,
        execution_timeout=timedelta(minutes=3),
    )
    t_check_input = PythonOperator(
        task_id="check_input_file",
        python_callable=check_input_file,
        execution_timeout=timedelta(minutes=3),
    )
    t_quality = PythonOperator(
        task_id="validate_quality",
        python_callable=validate_quality,
        execution_timeout=timedelta(minutes=3),
    )
    t_process = PythonOperator(
        task_id="simulate_processing",
        python_callable=simulate_processing,
        execution_timeout=timedelta(seconds=5),
    )
    t_optional = PythonOperator(
        task_id="optional_path",
        python_callable=optional_path,
        execution_timeout=timedelta(minutes=3),
    )

    t_join = EmptyOperator(
        task_id="join",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    t_summarize = PythonOperator(
        task_id="summarize_result",
        python_callable=summarize_result,
        # trigger_rule=TriggerRule.ALL_DONE,
        execution_timeout=timedelta(minutes=3),
    )

    # 선형 + fan-out/fan-in + 결과 요약
    (
        start
        >> t_prepare
        >> [
            t_branch_a,
            t_branch_b,
            t_check_input,
            t_quality,
            t_process,
            t_optional,
        ]
    )
    (
        [
            t_branch_a,
            t_branch_b,
            t_check_input,
            t_quality,
            t_process,
            t_optional,
        ]
        >> t_join
        >> t_summarize
        >> end
    )


