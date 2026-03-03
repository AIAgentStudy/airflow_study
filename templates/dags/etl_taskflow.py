"""TaskFlow API 기반 ETL 학습 예제.

extract -> transform -> load 흐름으로 태스크 간 데이터 전달과 함수형 DAG 작성 방식을 연습한다.
"""

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="study_taskflow_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["study", "week3"],
)
def taskflow_etl():
    @task()
    def extract() -> list[int]:
        return [1, 2, 3, 4, 5]

    @task()
    def transform(values: list[int]) -> list[int]:
        return [v * 10 for v in values]

    @task()
    def load(values: list[int]) -> int:
        print(f"적재_완료={values}")
        return len(values)

    load(transform(extract()))


taskflow_etl()
