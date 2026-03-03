"""Dynamic Task Mapping 학습 예제.

테이블 목록을 동적으로 펼쳐 여러 태스크 인스턴스를 생성하는 패턴을 연습한다.
"""

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="study_dynamic_mapping",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["study", "week6"],
)
def dynamic_mapping_dag():
    @task()
    def list_tables() -> list[str]:
        return ["customers", "orders", "products"]

    @task()
    def process_table(table: str) -> str:
        print(f"처리중={table}")
        return f"완료:{table}"

    process_table.expand(table=list_tables())


dynamic_mapping_dag()
