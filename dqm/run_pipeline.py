"""
run_pipeline.py
Entry point — runs the full data quality monitoring pipeline
for all registered tables and produces reports + alerts.
Usage: python run_pipeline.py
"""

import sys
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path

sys.path.insert(0, ".")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)

from checks.quality_checks   import run_all_checks, QualityConfig
from checks.quality_score    import compute_quality_score, build_score_record, append_score_history
from alerts.alert_manager    import AlertManager
from reports.report_generator import generate_summary_report, generate_multi_table_report
from pipelines.extract.extract_data import extract_table

TABLE_CONFIGS = [
    QualityConfig(
        table_name       = "claims",
        required_columns = ["claim_id","member_id","claim_status","billed_amount","claim_date"],
        not_null_columns = ["claim_id","member_id","claim_status","billed_amount"],
        unique_columns   = ["claim_id"],
        range_checks     = {"billed_amount":(0,500000), "processing_days":(0,365)},
        allowed_values   = {"claim_status":["approved","denied","pending","under_review"],
                            "claim_type":  ["medical","pharmacy","dental","vision","behavioral"]},
        regex_checks     = {"claim_date": r"\d{4}-\d{2}-\d{2}"},
    ),
    QualityConfig(
        table_name       = "members",
        required_columns = ["member_id","member_name","plan_type"],
        not_null_columns = ["member_id","member_name","plan_type"],
        unique_columns   = ["member_id"],
        allowed_values   = {"gender":    ["M","F","O","Unknown"],
                            "plan_type": ["PPO","HMO","EPO","HDHP","Medicaid"]},
    ),
]


def run_for_table(config: QualityConfig,
                  run_id: str,
                  alert_mgr: AlertManager) -> dict:
    """Run full quality pipeline for one table."""
    logger.info(f"--- Processing table: {config.table_name} ---")

    df      = extract_table(config.table_name, use_sample=True)
    results = run_all_checks(df, config)
    score   = compute_quality_score(results)
    record  = build_score_record(config.table_name, results, run_id=run_id)

    append_score_history(record)
    generate_summary_report(config.table_name, results, score)
    alert_mgr.raise_alert(config.table_name, score, results)

    return {"table_name": config.table_name, **score}


def main():
    run_id    = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    alert_mgr = AlertManager.__new__(AlertManager)
    alert_mgr.score_threshold = 90
    alert_mgr.email_enabled   = False
    alert_mgr.log_dir         = "reports/alerts"
    Path(alert_mgr.log_dir).mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting DQ pipeline run: {run_id}")
    all_scores = []

    for config in TABLE_CONFIGS:
        try:
            result = run_for_table(config, run_id, alert_mgr)
            all_scores.append(result)
        except Exception as e:
            logger.error(f"Pipeline failed for {config.table_name}: {e}")

    generate_multi_table_report(all_scores)
    logger.info(f"DQ pipeline complete. Run ID: {run_id}")


if __name__ == "__main__":
    main()
