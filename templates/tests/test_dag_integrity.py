"""DAG 템플릿 파일의 기본 무결성을 검증하는 테스트 예제.

- import 에러가 없는지 확인
- 필수 예제 DAG가 로드되는지 확인
"""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(include_examples=False)


def test_no_import_errors(dagbag: DagBag):
    assert dagbag.import_errors == {}


def test_expected_dags_present(dagbag: DagBag):
    expected = {
        "study_hello_airflow",
        "study_taskflow_etl",
        "study_dynamic_mapping",
    }
    missing = [dag_id for dag_id in expected if dag_id not in dagbag.dags]
    assert not missing, f"누락된 DAG: {missing}"
