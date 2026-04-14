"""
quality_score.py
Computes an overall data quality score (0–100) per dataset
and tracks the score over time for trending on dashboards.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WEIGHTS = {
    "schema_check":         0.20,
    "null_check":           0.25,
    "duplicate_check":      0.20,
    "range_check":          0.15,
    "allowed_values_check": 0.10,
    "regex_check":          0.10,
}

STATUS_SCORES = {"PASS": 1.0, "WARN": 0.5, "FAIL": 0.0}


def compute_quality_score(results_df: pd.DataFrame) -> dict:
    """
    Compute a weighted data quality score (0–100) from check results.
    Returns a dict with overall score, dimension scores, and grade.
    """
    if results_df.empty:
        return {"overall_score": 0, "grade": "F", "dimensions": {}}

    dimension_scores = {}
    for check_type, weight in WEIGHTS.items():
        subset = results_df[results_df["check_name"] == check_type]
        if subset.empty:
            continue
        dim_score = subset["status"].map(STATUS_SCORES).mean()
        dimension_scores[check_type] = round(dim_score * 100, 1)

    if not dimension_scores:
        return {"overall_score": 0, "grade": "F", "dimensions": {}}

    total_weight  = sum(WEIGHTS[k] for k in dimension_scores)
    overall_score = sum(
        dimension_scores[k] * WEIGHTS[k] for k in dimension_scores
    ) / total_weight

    overall_score = round(overall_score, 1)
    grade = (
        "A" if overall_score >= 95 else
        "B" if overall_score >= 85 else
        "C" if overall_score >= 70 else
        "D" if overall_score >= 55 else "F"
    )

    logger.info(f"Quality Score: {overall_score}/100 (Grade: {grade})")
    return {
        "overall_score": overall_score,
        "grade":         grade,
        "dimensions":    dimension_scores,
        "total_checks":  len(results_df),
        "passed":        int((results_df["status"] == "PASS").sum()),
        "warned":        int((results_df["status"] == "WARN").sum()),
        "failed":        int((results_df["status"] == "FAIL").sum()),
    }


def build_score_record(table_name: str,
                       results_df: pd.DataFrame,
                       run_id: str = None) -> dict:
    """Build a single score record suitable for appending to a history table."""
    score  = compute_quality_score(results_df)
    record = {
        "run_id":         run_id or datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
        "run_timestamp":  datetime.utcnow().isoformat(),
        "table_name":     table_name,
        "overall_score":  score["overall_score"],
        "grade":          score["grade"],
        "total_checks":   score["total_checks"],
        "passed":         score["passed"],
        "warned":         score["warned"],
        "failed":         score["failed"],
    }
    for dim, val in score.get("dimensions", {}).items():
        record[f"score_{dim}"] = val
    return record


def load_score_history(path: str = "data/processed/quality_score_history.csv") -> pd.DataFrame:
    """Load historical quality scores from CSV."""
    try:
        df = pd.read_csv(path, parse_dates=["run_timestamp"])
        logger.info(f"Loaded {len(df)} historical score records")
        return df
    except FileNotFoundError:
        logger.info("No score history found — starting fresh")
        return pd.DataFrame()


def append_score_history(new_record: dict,
                         path: str = "data/processed/quality_score_history.csv") -> pd.DataFrame:
    """Append a new score record to the history CSV."""
    import os
    history = load_score_history(path)
    new_row = pd.DataFrame([new_record])
    updated = pd.concat([history, new_row], ignore_index=True)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    updated.to_csv(path, index=False)
    logger.info(f"Score history updated: {len(updated)} records in {path}")
    return updated


def score_trend_summary(history_df: pd.DataFrame,
                        table_name: str) -> pd.DataFrame:
    """Summarise score trend for a specific table."""
    if history_df.empty:
        return pd.DataFrame()
    subset = history_df[history_df["table_name"] == table_name].copy()
    subset = subset.sort_values("run_timestamp")
    subset["score_delta"] = subset["overall_score"].diff().round(1)
    subset["trend"] = subset["score_delta"].apply(
        lambda x: "improving" if x > 0 else ("declining" if x < 0 else "stable")
        if pd.notna(x) else "n/a"
    )
    return subset[["run_timestamp", "overall_score", "grade",
                   "passed", "warned", "failed", "score_delta", "trend"]]


if __name__ == "__main__":
    import sys
    sys.path.append(".")
    from checks.quality_checks import run_all_checks, QualityConfig

    sample = pd.DataFrame({
        "claim_id":     ["C001", "C002", "C003", "C004", "C005"],
        "member_id":    ["M1", "M2", None, "M4", "M5"],
        "claim_status": ["approved", "denied", "approved", "approved", "pending"],
        "billed_amount":[1500, 200, 350, 900, 120],
    })
    config  = QualityConfig(
        table_name       = "claims",
        required_columns = ["claim_id", "member_id", "claim_status", "billed_amount"],
        not_null_columns = ["claim_id", "member_id"],
        unique_columns   = ["claim_id"],
        range_checks     = {"billed_amount": (0, 50000)},
        allowed_values   = {"claim_status": ["approved", "denied", "pending"]},
    )
    results = run_all_checks(sample, config)
    score   = compute_quality_score(results)
    print(f"\nQuality Score: {score['overall_score']}/100  Grade: {score['grade']}")
    print(f"Dimensions: {score['dimensions']}")
    record  = build_score_record("claims", results)
    history = append_score_history(record)
    print(f"\nScore history ({len(history)} records):")
    print(history.to_string())
