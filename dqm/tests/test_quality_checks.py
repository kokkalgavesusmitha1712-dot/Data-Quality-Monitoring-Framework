"""
test_quality_checks.py
Unit tests for the core data quality check framework.
Run with: pytest tests/ -v
"""

import pytest
import pandas as pd
import sys
sys.path.insert(0, ".")

from checks.quality_checks import (
    run_all_checks, QualityConfig,
    check_nulls, check_duplicates, check_ranges,
    check_allowed_values, check_schema, check_regex
)
from checks.quality_score import compute_quality_score


@pytest.fixture
def clean_df():
    return pd.DataFrame({
        "claim_id":     ["C001","C002","C003"],
        "member_id":    ["M1","M2","M3"],
        "claim_status": ["approved","denied","pending"],
        "billed_amount":[1500, 200, 350],
        "claim_date":   ["2024-01-01","2024-02-01","2024-03-01"],
    })


@pytest.fixture
def dirty_df():
    return pd.DataFrame({
        "claim_id":     ["C001","C002","C001"],     # duplicate C001
        "member_id":    ["M1", None, "M3"],          # null member_id
        "claim_status": ["approved","INVALID","pending"],  # invalid status
        "billed_amount":[ 1500, -99, 350],           # negative amount
        "claim_date":   ["2024-01-01","bad-date","2024-03-01"],
    })


@pytest.fixture
def base_config():
    return QualityConfig(
        table_name       = "claims_test",
        required_columns = ["claim_id","member_id","claim_status","billed_amount"],
        not_null_columns = ["claim_id","member_id"],
        unique_columns   = ["claim_id"],
        range_checks     = {"billed_amount": (0, 50000)},
        allowed_values   = {"claim_status": ["approved","denied","pending"]},
        regex_checks     = {"claim_date": r"\d{4}-\d{2}-\d{2}"},
    )


class TestSchemaCheck:
    def test_all_columns_present(self, clean_df, base_config):
        results = check_schema(clean_df, base_config)
        assert results[0].status == "PASS"

    def test_missing_column_fails(self, base_config):
        df = pd.DataFrame({"claim_id": ["C1"], "member_id": ["M1"]})
        results = check_schema(df, base_config)
        assert results[0].status == "FAIL"
        assert results[0].records_failed > 0


class TestNullCheck:
    def test_no_nulls_passes(self, clean_df, base_config):
        results = check_nulls(clean_df, base_config)
        assert all(r.status == "PASS" for r in results)

    def test_null_detected(self, dirty_df, base_config):
        results = check_nulls(dirty_df, base_config)
        member_result = next(r for r in results if r.column == "member_id")
        assert member_result.status in ("WARN", "FAIL")
        assert member_result.records_failed == 1


class TestDuplicateCheck:
    def test_no_duplicates_passes(self, clean_df, base_config):
        results = check_duplicates(clean_df, base_config)
        assert all(r.status == "PASS" for r in results)

    def test_duplicate_detected(self, dirty_df, base_config):
        results = check_duplicates(dirty_df, base_config)
        dup_result = next(r for r in results if r.column == "claim_id")
        assert dup_result.status in ("WARN", "FAIL")
        assert dup_result.records_failed > 0


class TestRangeCheck:
    def test_valid_range_passes(self, clean_df, base_config):
        results = check_ranges(clean_df, base_config)
        assert all(r.status == "PASS" for r in results)

    def test_out_of_range_fails(self, dirty_df, base_config):
        results = check_ranges(dirty_df, base_config)
        range_result = next(r for r in results if r.column == "billed_amount")
        assert range_result.status == "FAIL"
        assert range_result.records_failed == 1


class TestAllowedValuesCheck:
    def test_valid_values_pass(self, clean_df, base_config):
        results = check_allowed_values(clean_df, base_config)
        assert all(r.status == "PASS" for r in results)

    def test_invalid_value_fails(self, dirty_df, base_config):
        results = check_allowed_values(dirty_df, base_config)
        val_result = next(r for r in results if r.column == "claim_status")
        assert val_result.status == "FAIL"
        assert val_result.records_failed == 1


class TestQualityScore:
    def test_perfect_score_on_clean_data(self, clean_df, base_config):
        results = run_all_checks(clean_df, base_config)
        score   = compute_quality_score(results)
        assert score["overall_score"] == 100.0
        assert score["grade"] == "A"

    def test_low_score_on_dirty_data(self, dirty_df, base_config):
        results = run_all_checks(dirty_df, base_config)
        score   = compute_quality_score(results)
        assert score["overall_score"] < 90
        assert score["failed"] > 0

    def test_score_between_0_and_100(self, dirty_df, base_config):
        results = run_all_checks(dirty_df, base_config)
        score   = compute_quality_score(results)
        assert 0 <= score["overall_score"] <= 100


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
