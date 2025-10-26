# tests/test_data_quality.py
import pandas as pd
import pytest
from src.data_quality import DataQualityValidator

@pytest.fixture
def dq_validator():
    return DataQualityValidator()

def test_check_uniqueness_pass(dq_validator):
    df = pd.DataFrame({"id": [1, 2, 3]})
    assert dq_validator.check_uniqueness(df, ["id"], "test") == True
    assert len(dq_validator.results["passed"]) == 1

def test_check_uniqueness_fail(dq_validator):
    df = pd.DataFrame({"id": [1, 2, 2]})
    assert dq_validator.check_uniqueness(df, ["id"], "test") == False
    assert len(dq_validator.results["failed"]) == 1

def test_check_nulls_pass(dq_validator):
    df = pd.DataFrame({"id": [1, 2, 3]})
    assert dq_validator.check_nulls(df, ["id"], "test") == True
    assert len(dq_validator.results["passed"]) == 1

def test_check_nulls_fail(dq_validator):
    df = pd.DataFrame({"id": [1, 2, None]})
    assert dq_validator.check_nulls(df, ["id"], "test") == False
    assert len(dq_validator.results["failed"]) == 1

def test_referential_integrity_pass(dq_validator):
    parent_df = pd.DataFrame({"id": [1, 2, 3]})
    child_df = pd.DataFrame({"fk_id": [1, 2, 2]})
    assert dq_validator.check_referential_integrity(parent_df, child_df, "id", "fk_id", "test_rel") == True

def test_referential_integrity_fail(dq_validator):
    parent_df = pd.DataFrame({"id": [1, 2]})
    child_df = pd.DataFrame({"fk_id": [1, 3]}) # 3 is an orphan key
    assert dq_validator.check_referential_integrity(parent_df, child_df, "id", "fk_id", "test_rel") == False
    assert len(dq_validator.results["failed"]) == 1