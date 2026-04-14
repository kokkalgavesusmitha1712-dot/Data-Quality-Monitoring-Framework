"""
dq_monitoring_dag.py
Apache Airflow DAG — schedules daily data quality checks
across all registered tables and raises alerts on failures.

Schedule: daily at 6 AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

DEFAULT_ARGS = {
    "owner":            "sushmitha-kokkalgave",
    "depends_on_past":  False,
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id      = "data_quality_monitoring",
    description = "Daily data quality checks across all registered tables",
    schedule_interval = "0 6 * * *",     # 6 AM UTC daily
    start_date  = days_ago(1),
    default_args = DEFAULT_ARGS,
    catchup     = False,
    tags        = ["data-quality", "monitoring", "etl"],
)


def run_claims_quality_check(**context) -> dict:
    """Quality checks for the claims table."""
    from checks.quality_checks import run_all_checks, QualityConfig
    from checks.quality_score  import compute_quality_score, build_score_record, append_score_history
    from alerts.alert_manager  import AlertManager
    from pipelines.extract.extract_data import extract_table

    df     = extract_table("claims")
    config = QualityConfig(
        table_name       = "claims",
        required_columns = ["claim_id","member_id","provider_id","claim_date",
                            "claim_type","billed_amount","claim_status"],
        not_null_columns = ["claim_id","member_id","claim_status","billed_amount"],
        unique_columns   = ["claim_id"],
        range_checks     = {"billed_amount":(0,500000), "processing_days":(0,365)},
        allowed_values   = {"claim_status":["approved","denied","pending","under_review"],
                            "claim_type":  ["medical","pharmacy","dental","vision","behavioral"]},
        regex_checks     = {"claim_date": r"\d{4}-\d{2}-\d{2}"},
    )
    results = run_all_checks(df, config)
    score   = compute_quality_score(results)
    record  = build_score_record("claims", results,
                                  run_id=context["run_id"])
    append_score_history(record)
    AlertManager(config_path="config/config.yaml").raise_alert(
        "claims", score, results
    )
    context["ti"].xcom_push(key="claims_score", value=score["overall_score"])
    return score


def run_members_quality_check(**context) -> dict:
    """Quality checks for the members table."""
    from checks.quality_checks import run_all_checks, QualityConfig
    from checks.quality_score  import compute_quality_score, build_score_record, append_score_history
    from alerts.alert_manager  import AlertManager
    from pipelines.extract.extract_data import extract_table

    df     = extract_table("members")
    config = QualityConfig(
        table_name       = "members",
        required_columns = ["member_id","member_name","date_of_birth","gender","plan_type"],
        not_null_columns = ["member_id","member_name","plan_type"],
        unique_columns   = ["member_id"],
        allowed_values   = {"gender":    ["M","F","O","Unknown"],
                            "plan_type": ["PPO","HMO","EPO","HDHP","Medicaid"]},
    )
    results = run_all_checks(df, config)
    score   = compute_quality_score(results)
    record  = build_score_record("members", results, run_id=context["run_id"])
    append_score_history(record)
    AlertManager(config_path="config/config.yaml").raise_alert(
        "members", score, results
    )
    context["ti"].xcom_push(key="members_score", value=score["overall_score"])
    return score


def run_revenue_quality_check(**context) -> dict:
    """Quality checks for the revenue table."""
    from checks.quality_checks import run_all_checks, QualityConfig
    from checks.quality_score  import compute_quality_score, build_score_record, append_score_history
    from alerts.alert_manager  import AlertManager
    from pipelines.extract.extract_data import extract_table

    df     = extract_table("revenue_actuals")
    config = QualityConfig(
        table_name       = "revenue_actuals",
        required_columns = ["period","department","region","actual_revenue"],
        not_null_columns = ["period","department","actual_revenue"],
        range_checks     = {"actual_revenue":(0,10_000_000)},
        allowed_values   = {"department": ["Inpatient","Outpatient","Pharmacy",
                                           "Diagnostics","Behavioral Health"]},
    )
    results = run_all_checks(df, config)
    score   = compute_quality_score(results)
    record  = build_score_record("revenue_actuals", results, run_id=context["run_id"])
    append_score_history(record)
    AlertManager(config_path="config/config.yaml").raise_alert(
        "revenue_actuals", score, results
    )
    context["ti"].xcom_push(key="revenue_score", value=score["overall_score"])
    return score


def generate_daily_report(**context) -> None:
    """Pull all scores from XCom and generate consolidated report."""
    from reports.report_generator import generate_multi_table_report

    ti     = context["ti"]
    scores = [
        {"table_name": "claims",          "overall_score": ti.xcom_pull(key="claims_score")},
        {"table_name": "members",         "overall_score": ti.xcom_pull(key="members_score")},
        {"table_name": "revenue_actuals", "overall_score": ti.xcom_pull(key="revenue_score")},
    ]
    generate_multi_table_report(scores)


# ── Task definitions ──────────────────────────────────────────────

t_claims = PythonOperator(
    task_id         = "check_claims",
    python_callable = run_claims_quality_check,
    provide_context = True,
    dag             = dag,
)

t_members = PythonOperator(
    task_id         = "check_members",
    python_callable = run_members_quality_check,
    provide_context = True,
    dag             = dag,
)

t_revenue = PythonOperator(
    task_id         = "check_revenue",
    python_callable = run_revenue_quality_check,
    provide_context = True,
    dag             = dag,
)

t_report = PythonOperator(
    task_id         = "generate_daily_report",
    python_callable = generate_daily_report,
    provide_context = True,
    dag             = dag,
)

# ── DAG structure: all checks run in parallel, then report ───────
[t_claims, t_members, t_revenue] >> t_report
