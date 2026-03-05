from __future__ import annotations
# 위 import는 최신 타입 힌트 문법(예: list[str]) 호환성을 높이기 위해 사용합니다.

import json
# 크롤링 결과/정제 결과를 JSON으로 파일에 저장하기 위해 사용합니다.

from datetime import datetime, timedelta
# DAG 시작 시각, 재시도 간격, 실행 제한시간을 지정하기 위해 사용합니다.

from pathlib import Path
# 운영체제에 안전한 방식으로 파일 경로를 다루기 위해 사용합니다.

from airflow.sdk import DAG
# Airflow 3.x 공개 SDK 경로에서 DAG 객체를 가져옵니다.

from airflow.exceptions import AirflowFailException
# 데이터 품질 검증 실패를 명시적으로 실패 처리하기 위해 사용합니다.

from airflow.providers.standard.operators.empty import EmptyOperator
# Airflow 3.x standard provider 경로에서 EmptyOperator를 가져옵니다.

from airflow.providers.standard.operators.python import PythonOperator
# Airflow 3.x standard provider 경로에서 PythonOperator를 가져옵니다.

from airflow.utils.trigger_rule import TriggerRule
# 실패 여부와 무관하게 실행할 태스크 규칙을 지정하기 위해 사용합니다.

DEFAULT_ARGS = {
    # DAG의 기본 owner를 지정합니다.
    "owner": "data-platform",
    # 과거 실행 성공 여부에 의존하지 않도록 설정합니다.
    "depends_on_past": False,
    # 실패 시 재시도 횟수를 지정합니다.
    "retries": 2,
    # 재시도 간 대기 시간을 2분으로 설정합니다.
    "retry_delay": timedelta(minutes=2),
}


def _base_dir() -> Path:
    # DAG 파일 위치 기준으로 상대 경로를 계산하기 위한 헬퍼입니다.
    return Path(__file__).resolve().parent


def _data_dir() -> Path:
    # 원본/정제 데이터를 저장할 로컬 디렉터리 경로를 구성합니다.
    return _base_dir() / "_local_data" / "crawl_demo"


def _raw_file(ds: str) -> Path:
    # 실행일(ds)별 원본 데이터 파일 경로를 반환합니다.
    return _data_dir() / f"raw_{ds}.json"


def _clean_file(ds: str) -> Path:
    # 실행일(ds)별 정제 데이터 파일 경로를 반환합니다.
    return _data_dir() / f"clean_{ds}.json"


def _mart_file(ds: str) -> Path:
    # 실행일(ds)별 적재 결과 파일 경로를 반환합니다.
    return _data_dir() / f"mart_{ds}.json"


def crawl_source(**context) -> str:
    # Airflow 컨텍스트에서 실행일(YYYY-MM-DD)을 가져옵니다.
    ds = context["ds"]
    # 원본 데이터 저장 디렉터리를 생성합니다(이미 있으면 유지).
    _data_dir().mkdir(parents=True, exist_ok=True)
    # 크롤링 결과를 가정한 샘플 레코드를 만듭니다.
    raw_records = [
        # 정상 레코드 예시입니다.
        {"id": 1, "title": "Airflow Intro", "price": "12000", "category": "book"},
        # 공백/대문자/문자열 숫자를 포함해 정제 로직을 보여주는 레코드입니다.
        {"id": 2, "title": "  Python Operator  ", "price": "9000", "category": "Book"},
        # 결측값이 포함된 레코드로 필터링 예시를 보여줍니다.
        {"id": 3, "title": "", "price": None, "category": "misc"},
    ]
    # 실행일 기준 원본 파일 경로를 계산합니다.
    raw_path = _raw_file(ds)
    # JSON 파일로 원본 데이터를 UTF-8 인코딩으로 저장합니다.
    raw_path.write_text(json.dumps(raw_records, ensure_ascii=False, indent=2), encoding="utf-8")
    # 로그에 저장 경로와 건수를 출력해 관측성을 높입니다.
    print(f"[crawl] saved raw file: {raw_path} | rows={len(raw_records)}")
    # downstream 태스크에서 읽을 수 있도록 경로 문자열을 반환(XCom)합니다.
    return str(raw_path)


def clean_records(**context) -> str:
    # 현재 태스크 인스턴스를 가져와 XCom 조회에 사용합니다.
    ti = context["ti"]
    # upstream(crawl_source)가 반환한 원본 파일 경로를 XCom에서 읽습니다.
    raw_path = Path(ti.xcom_pull(task_ids="crawl_source"))
    # 파일 내용을 JSON으로 읽어 파이썬 리스트로 변환합니다.
    raw_records = json.loads(raw_path.read_text(encoding="utf-8"))
    # 정제된 레코드를 담을 리스트를 초기화합니다.
    cleaned = []

    # 각 원본 레코드를 순회하면서 정제를 수행합니다.
    for row in raw_records:
        # 제목 문자열 앞뒤 공백을 제거합니다.
        title = str(row.get("title", "")).strip()
        # 가격이 없으면 기본값 0으로 보정하고, 있으면 정수형으로 변환합니다.
        price = int(row.get("price") or 0)
        # 카테고리는 소문자 표준값으로 통일합니다.
        category = str(row.get("category", "unknown")).strip().lower()

        # 필수값(제목/가격)이 비정상인 레코드는 버립니다.
        if not title or price <= 0:
            # 어떤 레코드가 제거됐는지 로그를 남겨 디버깅을 쉽게 합니다.
            print(f"[clean] dropped row: {row}")
            # 다음 레코드로 넘어갑니다.
            continue

        # 유효한 레코드는 표준 스키마로 재구성하여 리스트에 추가합니다.
        cleaned.append({"id": row["id"], "title": title, "price": price, "category": category})

    # 실행일 기준 정제 파일 경로를 계산합니다.
    clean_path = _clean_file(context["ds"])
    # 정제 결과를 JSON 파일로 저장합니다.
    clean_path.write_text(json.dumps(cleaned, ensure_ascii=False, indent=2), encoding="utf-8")
    # 정제 완료 로그를 남깁니다.
    print(f"[clean] saved clean file: {clean_path} | rows={len(cleaned)}")
    # downstream 태스크가 사용할 정제 파일 경로를 반환합니다.
    return str(clean_path)


def validate_quality(**context) -> dict:
    # 현재 태스크 인스턴스를 가져옵니다.
    ti = context["ti"]
    # upstream(clean_records)에서 반환한 정제 파일 경로를 읽어옵니다.
    clean_path = Path(ti.xcom_pull(task_ids="clean_records"))
    # 정제 데이터를 JSON으로 읽습니다.
    cleaned = json.loads(clean_path.read_text(encoding="utf-8"))

    # 검증용 집계 지표를 계산합니다.
    metrics = {
        # 정제 후 총 레코드 수를 계산합니다.
        "row_count": len(cleaned),
        # 가격 합계를 계산합니다.
        "price_sum": sum(int(r["price"]) for r in cleaned),
    }

    # 최소 1건도 없으면 파이프라인을 실패로 처리합니다.
    if metrics["row_count"] == 0:
        # 실패 이유를 명확히 남깁니다.
        raise AirflowFailException("[validate] cleaned row_count is 0")

    # 총액이 0 이하이면 비정상으로 판단해 실패 처리합니다.
    if metrics["price_sum"] <= 0:
        # 실패 이유를 명확히 남깁니다.
        raise AirflowFailException("[validate] cleaned price_sum must be > 0")

    # 검증 통과 로그를 남깁니다.
    print(f"[validate] metrics={metrics}")
    # load 단계에서 재사용할 수 있도록 지표를 XCom으로 반환합니다.
    return metrics


def load_mart(**context) -> str:
    # 태스크 인스턴스를 가져와 여러 XCom 값을 조회합니다.
    ti = context["ti"]
    # 실행일 문자열을 가져옵니다.
    ds = context["ds"]
    # 정제 파일 경로를 조회합니다.
    clean_path = Path(ti.xcom_pull(task_ids="clean_records"))
    # 품질 검증 지표를 조회합니다.
    metrics = ti.xcom_pull(task_ids="validate_quality")

    # 정제 데이터를 읽습니다.
    cleaned = json.loads(clean_path.read_text(encoding="utf-8"))
    # 마트 적재용 payload를 구성합니다.
    mart_payload = {
        # 어떤 실행일 배치인지 기록합니다.
        "batch_date": ds,
        # 적재된 데이터 본문입니다.
        "records": cleaned,
        # 검증 지표를 함께 저장해 추적 가능성을 높입니다.
        "quality_metrics": metrics,
    }

    # 실행일 기준 마트 파일 경로를 계산합니다.
    mart_path = _mart_file(ds)
    # 마트 payload를 JSON 파일로 저장합니다.
    mart_path.write_text(json.dumps(mart_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    # 적재 완료 로그를 남깁니다.
    print(f"[load] saved mart file: {mart_path} | rows={len(cleaned)}")
    # 후속 알림 태스크에서 사용할 경로를 반환합니다.
    return str(mart_path)


def notify_result(**context) -> None:
    # 태스크 인스턴스를 가져옵니다.
    ti = context["ti"]
    # 적재된 최종 파일 경로를 가져옵니다.
    mart_path = ti.xcom_pull(task_ids="load_mart")
    # 품질 검증 지표를 가져옵니다.
    metrics = ti.xcom_pull(task_ids="validate_quality")
    # 운영 알림 대신 콘솔 로그로 요약 알림을 출력합니다.
    print(f"[notify] mart_path={mart_path} | metrics={metrics}")


with DAG(
    # DAG 고유 식별자입니다.
    dag_id="template_crawl_clean_pipeline",
    # DAG 기본 파라미터를 적용합니다.
    default_args=DEFAULT_ARGS,
    # 학습/운영에서 DAG 목적을 쉽게 이해하도록 설명을 작성합니다.
    description="실무형 템플릿: crawl -> clean -> validate -> load -> notify",
    # 매일 오전 6시에 실행되도록 스케줄을 설정합니다.
    schedule="0 6 * * *",
    # 시작일을 명시해 스케줄 계산 기준점을 고정합니다.
    start_date=datetime(2026, 3, 5),
    # 과거 미실행 구간을 자동 백필하지 않도록 설정합니다.
    catchup=False,
    # 동시에 여러 실행이 겹치지 않도록 1개로 제한합니다.
    max_active_runs=1,
    # UI에서 분류하기 쉽도록 태그를 지정합니다.
    tags=["template", "crawl", "pythonoperator"],
) as dag:
    # 파이프라인 시작을 나타내는 더미 태스크입니다.
    start = EmptyOperator(task_id="start")

    # 원본 데이터 수집(크롤링) 태스크입니다.
    t_crawl = PythonOperator(
        # 태스크 고유 ID를 지정합니다.
        task_id="crawl_source",
        # 실행할 파이썬 함수를 지정합니다.
        python_callable=crawl_source,
        # 무한 실행을 방지하기 위해 타임아웃을 설정합니다.
        execution_timeout=timedelta(minutes=5),
    )

    # 원본 데이터 정제 태스크입니다.
    t_clean = PythonOperator(
        # 태스크 고유 ID를 지정합니다.
        task_id="clean_records",
        # 실행할 파이썬 함수를 지정합니다.
        python_callable=clean_records,
        # 무한 실행을 방지하기 위해 타임아웃을 설정합니다.
        execution_timeout=timedelta(minutes=5),
    )

    # 데이터 품질 검증 태스크입니다.
    t_validate = PythonOperator(
        # 태스크 고유 ID를 지정합니다.
        task_id="validate_quality",
        # 실행할 파이썬 함수를 지정합니다.
        python_callable=validate_quality,
        # 무한 실행을 방지하기 위해 타임아웃을 설정합니다.
        execution_timeout=timedelta(minutes=3),
    )

    # 정제/검증 완료 데이터를 마트 형태로 적재하는 태스크입니다.
    t_load = PythonOperator(
        # 태스크 고유 ID를 지정합니다.
        task_id="load_mart",
        # 실행할 파이썬 함수를 지정합니다.
        python_callable=load_mart,
        # 무한 실행을 방지하기 위해 타임아웃을 설정합니다.
        execution_timeout=timedelta(minutes=5),
    )

    # 결과 알림 태스크입니다.
    t_notify = PythonOperator(
        # 태스크 고유 ID를 지정합니다.
        task_id="notify_result",
        # 실행할 파이썬 함수를 지정합니다.
        python_callable=notify_result,
        # upstream 실패 여부와 상관없이 항상 알림이 돌도록 설정합니다.
        trigger_rule=TriggerRule.ALL_DONE,
        # 알림 태스크의 실행 제한시간을 설정합니다.
        execution_timeout=timedelta(minutes=2),
    )

    # 파이프라인 종료를 나타내는 더미 태스크입니다.
    end = EmptyOperator(task_id="end")

    # 실행 순서를 선형으로 정의합니다.
    start >> t_crawl >> t_clean >> t_validate >> t_load >> t_notify >> end
