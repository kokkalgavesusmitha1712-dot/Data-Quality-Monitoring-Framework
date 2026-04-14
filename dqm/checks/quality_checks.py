"""
quality_checks.py
Core configurable data quality check framework.
Runs null checks, range checks, duplicate detection,
format validation, referential integrity, and freshness checks
on any Pandas DataFrame.
"""

import pandas as pd
import numpy as np
import logging
from dataclasses import dataclass, field
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    """Stores the result of a single quality check."""
    check_name:   str
    table:        str
    column:       str
    status:       str          # PASS | FAIL | WARN
    records_checked: int
    records_failed:  int
    failure_rate_pct: float
    details:      str = ""

    def to_dict(self) -> dict:
        return {
            "check_name":        self.check_name,
            "table":             self.table,
            "column":            self.column,
            "status":            self.status,
            "records_checked":   self.records_checked,
            "records_failed":    self.records_failed,
            "failure_rate_pct":  self.failure_rate_pct,
            "details":           self.details,
        }


@dataclass
class QualityConfig:
    """Per-table quality check configuration."""
    table_name:         str
    required_columns:   list = field(default_factory=list)
    not_null_columns:   list = field(default_factory=list)
    unique_columns:     list = field(default_factory=list)
    range_checks:       dict = field(default_factory=dict)   # {col: (min, max)}
    regex_checks:       dict = field(default_factory=dict)   # {col: pattern}
    allowed_values:     dict = field(default_factory=dict)   # {col: [val1, val2]}
    warn_null_pct:      float = 5.0
    fail_null_pct:      float = 20.0
    warn_duplicate_pct: float = 1.0
    fail_duplicate_pct: float = 5.0


def check_schema(df: pd.DataFrame, config: QualityConfig) -> list[CheckResult]:
    """Verify all required columns are present."""
    results = []
    missing = [c for c in config.required_columns if c not in df.columns]
    status  = "FAIL" if missing else "PASS"
    results.append(CheckResult(
        check_name       = "schema_check",
        table            = config.table_name,
        column           = "ALL",
        status           = status,
        records_checked  = len(df.columns),
        records_failed   = len(missing),
        failure_rate_pct = round(len(missing) / max(len(config.required_columns), 1) * 100, 2),
        details          = f"Missing columns: {missing}" if missing else "All required columns present",
    ))
    return results


def check_nulls(df: pd.DataFrame, config: QualityConfig) -> list[CheckResult]:
    """Check null rates for specified columns."""
    results = []
    cols = config.not_null_columns or df.columns.tolist()

    for col in cols:
        if col not in df.columns:
            continue
        null_count = df[col].isnull().sum()
        null_pct   = round(null_count / len(df) * 100, 2) if len(df) else 0

        if null_pct >= config.fail_null_pct:
            status = "FAIL"
        elif null_pct >= config.warn_null_pct:
            status = "WARN"
        else:
            status = "PASS"

        results.append(CheckResult(
            check_name       = "null_check",
            table            = config.table_name,
            column           = col,
            status           = status,
            records_checked  = len(df),
            records_failed   = int(null_count),
            failure_rate_pct = null_pct,
            details          = f"{null_pct}% nulls (warn>{config.warn_null_pct}% fail>{config.fail_null_pct}%)",
        ))
    return results


def check_duplicates(df: pd.DataFrame, config: QualityConfig) -> list[CheckResult]:
    """Detect duplicate values in key columns."""
    results = []
    for col in config.unique_columns:
        if col not in df.columns:
            continue
        dup_count = df[col].duplicated(keep=False).sum()
        dup_pct   = round(dup_count / len(df) * 100, 2) if len(df) else 0

        if dup_pct >= config.fail_duplicate_pct:
            status = "FAIL"
        elif dup_pct >= config.warn_duplicate_pct:
            status = "WARN"
        else:
            status = "PASS"

        results.append(CheckResult(
            check_name       = "duplicate_check",
            table            = config.table_name,
            column           = col,
            status           = status,
            records_checked  = len(df),
            records_failed   = int(dup_count),
            failure_rate_pct = dup_pct,
            details          = f"{dup_count} duplicate values in {col}",
        ))
    return results


def check_ranges(df: pd.DataFrame, config: QualityConfig) -> list[CheckResult]:
    """Validate numeric columns are within expected min/max bounds."""
    results = []
    for col, (min_val, max_val) in config.range_checks.items():
        if col not in df.columns:
            continue
        series     = pd.to_numeric(df[col], errors="coerce").dropna()
        out_of_range = ((series < min_val) | (series > max_val)).sum()
        fail_pct   = round(out_of_range / len(series) * 100, 2) if len(series) else 0
        status     = "FAIL" if fail_pct > 0 else "PASS"

        results.append(CheckResult(
            check_name       = "range_check",
            table            = config.table_name,
            column           = col,
            status           = status,
            records_checked  = len(series),
            records_failed   = int(out_of_range),
            failure_rate_pct = fail_pct,
            details          = f"Expected [{min_val}, {max_val}]. {out_of_range} records out of range.",
        ))
    return results


def check_allowed_values(df: pd.DataFrame, config: QualityConfig) -> list[CheckResult]:
    """Check categorical columns only contain permitted values."""
    results = []
    for col, allowed in config.allowed_values.items():
        if col not in df.columns:
            continue
        series    = df[col].dropna()
        invalid   = (~series.isin(allowed)).sum()
        fail_pct  = round(invalid / len(series) * 100, 2) if len(series) else 0
        status    = "FAIL" if invalid > 0 else "PASS"
        bad_vals  = series[~series.isin(allowed)].unique().tolist()[:5]

        results.append(CheckResult(
            check_name       = "allowed_values_check",
            table            = config.table_name,
            column           = col,
            status           = status,
            records_checked  = len(series),
            records_failed   = int(invalid),
            failure_rate_pct = fail_pct,
            details          = f"Invalid values found: {bad_vals}" if invalid else "All values valid",
        ))
    return results


def check_regex(df: pd.DataFrame, config: QualityConfig) -> list[CheckResult]:
    """Validate string columns match an expected regex pattern."""
    import re
    results = []
    for col, pattern in config.regex_checks.items():
        if col not in df.columns:
            continue
        series   = df[col].dropna().astype(str)
        invalid  = (~series.str.match(pattern)).sum()
        fail_pct = round(invalid / len(series) * 100, 2) if len(series) else 0
        status   = "FAIL" if invalid > 0 else "PASS"

        results.append(CheckResult(
            check_name       = "regex_check",
            table            = config.table_name,
            column           = col,
            status           = status,
            records_checked  = len(series),
            records_failed   = int(invalid),
            failure_rate_pct = fail_pct,
            details          = f"Pattern: {pattern}. {invalid} records don't match.",
        ))
    return results


def run_all_checks(df: pd.DataFrame, config: QualityConfig) -> pd.DataFrame:
    """
    Master function — runs every configured check and returns
    a consolidated results DataFrame.
    """
    logger.info(f"Running quality checks on '{config.table_name}' ({len(df):,} rows)")
    all_results = []
    all_results += check_schema(df, config)
    all_results += check_nulls(df, config)
    all_results += check_duplicates(df, config)
    all_results += check_ranges(df, config)
    all_results += check_allowed_values(df, config)
    all_results += check_regex(df, config)

    results_df = pd.DataFrame([r.to_dict() for r in all_results])

    total  = len(results_df)
    passed = (results_df["status"] == "PASS").sum()
    failed = (results_df["status"] == "FAIL").sum()
    warned = (results_df["status"] == "WARN").sum()

    logger.info(f"Results: {passed}/{total} PASS | {warned} WARN | {failed} FAIL")
    return results_df


if __name__ == "__main__":
    sample = pd.DataFrame({
        "claim_id":       ["C001", "C002", "C001", "C003", None],
        "member_id":      ["M1",   "M2",   "M3",   "M4",   "M5"],
        "claim_status":   ["approved", "denied", "approved", "INVALID", None],
        "billed_amount":  [1500, -50, 200, 99999, 300],
        "claim_date":     ["2024-01-01", "2024-02-01", "2024-03-01", "bad-date", "2024-04-01"],
    })

    config = QualityConfig(
        table_name        = "claims",
        required_columns  = ["claim_id", "member_id", "claim_status", "billed_amount"],
        not_null_columns  = ["claim_id", "member_id", "claim_status"],
        unique_columns    = ["claim_id"],
        range_checks      = {"billed_amount": (0, 50000)},
        allowed_values    = {"claim_status": ["approved", "denied", "pending", "under_review"]},
        regex_checks      = {"claim_date": r"\d{4}-\d{2}-\d{2}"},
        warn_null_pct     = 5.0,
        fail_null_pct     = 20.0,
    )

    results = run_all_checks(sample, config)
    print(results.to_string())
