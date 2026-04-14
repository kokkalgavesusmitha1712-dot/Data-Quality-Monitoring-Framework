"""
report_generator.py
Generates an automated data quality summary report as a CSV
and a human-readable text summary — ready for Power BI / Tableau
or email distribution.
"""

import pandas as pd
import logging
from datetime import datetime
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def generate_summary_report(table_name: str,
                             results_df: pd.DataFrame,
                             score: dict,
                             output_dir: str = "reports") -> str:
    """
    Build a text + CSV summary report for a single table run.
    Returns the path to the saved CSV report.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    ts    = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"{output_dir}/dq_report_{table_name}_{ts}.csv"
    results_df.to_csv(fname, index=False)

    print("\n" + "=" * 60)
    print(f"  DATA QUALITY REPORT — {table_name.upper()}")
    print(f"  Run: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)
    print(f"  Overall Score : {score['overall_score']}/100  |  Grade: {score['grade']}")
    print(f"  Total Checks  : {score['total_checks']}")
    print(f"  Passed        : {score['passed']}")
    print(f"  Warned        : {score['warned']}")
    print(f"  Failed        : {score['failed']}")
    print("-" * 60)

    if score["failed"] > 0:
        print("  FAILED CHECKS:")
        for _, row in results_df[results_df["status"] == "FAIL"].iterrows():
            print(f"    ✗ [{row['check_name']}] col={row['column']}  "
                  f"failed={row['records_failed']:,}  {row['details']}")

    if score["warned"] > 0:
        print("  WARNINGS:")
        for _, row in results_df[results_df["status"] == "WARN"].iterrows():
            print(f"    ! [{row['check_name']}] col={row['column']}  "
                  f"failed={row['records_failed']:,}  {row['details']}")

    print(f"\n  Full report saved → {fname}")
    print("=" * 60 + "\n")
    return fname


def generate_multi_table_report(all_scores: list[dict],
                                 output_dir: str = "reports") -> str:
    """
    Generate a consolidated report across multiple tables.
    all_scores: list of dicts with table_name, score, timestamp.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    ts    = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    fname = f"{output_dir}/dq_multi_table_report_{ts}.csv"

    df = pd.DataFrame(all_scores)
    df.to_csv(fname, index=False)

    print("\n" + "=" * 60)
    print("  MULTI-TABLE DATA QUALITY SUMMARY")
    print(f"  Run: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 60)
    for rec in sorted(all_scores, key=lambda x: x["overall_score"]):
        bar = "█" * int(rec["overall_score"] / 5)
        print(f"  {rec['table_name']:30s} {rec['overall_score']:5.1f}/100  {bar}")
    print(f"\n  Consolidated report → {fname}")
    print("=" * 60 + "\n")
    return fname


if __name__ == "__main__":
    import sys
    sys.path.append(".")
    from checks.quality_checks import run_all_checks, QualityConfig
    from checks.quality_score  import compute_quality_score

    sample = pd.DataFrame({
        "claim_id":     ["C001","C002","C003","C004","C005"],
        "member_id":    ["M1","M2",None,"M4","M5"],
        "claim_status": ["approved","denied","approved","approved","pending"],
        "billed_amount":[1500,200,350,-10,120],
    })
    config = QualityConfig(
        table_name       = "claims",
        required_columns = ["claim_id","member_id","claim_status","billed_amount"],
        not_null_columns = ["claim_id","member_id"],
        unique_columns   = ["claim_id"],
        range_checks     = {"billed_amount":(0,50000)},
        allowed_values   = {"claim_status":["approved","denied","pending"]},
    )
    results = run_all_checks(sample, config)
    score   = compute_quality_score(results)
    generate_summary_report("claims", results, score)
