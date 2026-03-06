"""3주차 학습용 DAG: TaskFlow API와 XCom 계약을 익히는 파일.

학습 포인트:
- @dag/@task 기반 TaskFlow 스타일 작성법
- 태스크 간 데이터 전달(XCom)의 타입/키 계약이 깨질 때의 실패 양상
- Jinja 렌더링 오류와 런타임 오류를 구분해 읽는 로그 분석 습관
"""

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from jinja2 import StrictUndefined


@dag(
    dag_id="study_taskflow_etl",
    start_date=datetime(2026, 3, 4),
    schedule="@daily",
    catchup=False,
    template_undefined=StrictUndefined,
    default_args={"owner": "study", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["week3", "taskflow", "xcom"],
)
def study_taskflow_etl():
    # 기본은 정상 경로(happy_path). UI Trigger DAG의 conf로 덮어쓰기 가능.
    # 예시 conf (A: XCom 키 누락)
    # {"scenario": "xcom_key_missing", "rows": 100}
    # 예시 conf (B: Jinja 템플릿 렌더 오류)
    # {"scenario": "template_render_error", "rows": 100}
    # 예시 conf (C: payload 타입 변경 -> downstream 처리 실패)
    # {"scenario": "payload_type_error", "rows": 100}
    DEFAULT_SCENARIO = "happy_path"

    @task(execution_timeout=timedelta(minutes=3))
    def extract(dag_run=None):
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        rows = int(conf.get("rows", 100))
        print(f"추출 완료 | rows={rows}")
        return {"rows": rows, "source": "demo"}

    @task(execution_timeout=timedelta(minutes=3))
    def transform(extracted: dict, template_probe: str, dag_run=None):
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        scenario = conf.get("scenario", DEFAULT_SCENARIO)

        # 시나리오 A: 없는 key 참조로 XCom contract 깨짐 재현
        if scenario == "xcom_key_missing":
            _ = extracted["missing_key"]

        # 시나리오 B: template_probe는 실행 전에 Jinja 렌더링됨.
        # scenario=template_render_error 일 때 unknown_var 렌더 단계에서 실패.
        _ = template_probe

        # 시나리오 C: payload 타입을 dict -> str로 바꿔 downstream 실패 재현
        if scenario == "payload_type_error":
            return "not-a-dict"

        transformed = {"rows": extracted["rows"], "status": "ok"}
        print(f"변환 완료 | payload={transformed}")
        return transformed

    @task(execution_timeout=timedelta(minutes=3))
    def load(payload, dag_run=None):
        if not isinstance(payload, dict):
            raise AirflowFailException("XCom payload 타입 오류: dict가 아님")
        print(f"적재 완료 | rows={payload['rows']}")

    transformed = transform(
        extract(),
        template_probe="{{ unknown_var if dag_run.conf.get('scenario') == 'template_render_error' else 'ok' }}",
    )
    load(transformed)


study_taskflow_etl()


