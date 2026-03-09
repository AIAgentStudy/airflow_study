"""6주차 학습용 DAG: Dynamic Task Mapping과 Dataset 트리거를 익히는 파일.

학습 포인트:
- list_tables 결과를 기반으로 process_table 태스크를 동적으로 확장(expand)하는 방법
- 특정 매핑 항목만 실패시키며 부분 실패 관찰
- producer DAG의 Dataset 갱신으로 consumer DAG가 자동 실행되는 흐름 이해
"""

from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException

STUDY_DATASET = Dataset("study://week6/processed")


@dag(
    dag_id="study_dynamic_mapping",
    start_date=datetime(2026, 3, 4),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "study", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["week6", "mapping", "dataset"],
)
def study_dynamic_mapping():
    @task(execution_timeout=timedelta(minutes=3), outlets=[STUDY_DATASET])
    def list_tables(dag_run=None):
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        tables = conf.get("tables", ["customers", "orders", "products"])
        print(f"매핑 대상 테이블: {tables}")
        return tables

    @task(execution_timeout=timedelta(minutes=3))
    def process_table(table_name: str, dag_run=None):
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        bad_table = conf.get("bad_table", "")
        if table_name == bad_table:
            raise AirflowFailException(f"매핑 항목 실패: {table_name}")
        print(f"테이블 처리 성공: {table_name}")

    process_table.expand(table_name=list_tables())


@dag(
    dag_id="study_week6_dataset_consumer",
    start_date=datetime(2026, 3, 4),
    schedule=[STUDY_DATASET],
    catchup=False,
    default_args={"owner": "study", "retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["week6", "dataset", "consumer"],
)
def study_week6_dataset_consumer():
    @task(execution_timeout=timedelta(minutes=3))
    def consume():
        print("Dataset 트리거로 소비 DAG 실행")

    consume()


study_dynamic_mapping()
study_week6_dataset_consumer()


