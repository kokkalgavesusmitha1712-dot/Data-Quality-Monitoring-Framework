"""
alert_manager.py
Sends alerts when data quality checks fail or scores drop below thresholds.
Supports email (SMTP) and console logging.
Extend with Slack / Teams webhooks as needed.
"""

import smtplib
import logging
import json
import yaml
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AlertManager:
    def __init__(self, config_path: str = "config/config.yaml"):
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        self.alert_cfg        = cfg.get("alerts", {})
        self.score_threshold  = self.alert_cfg.get("score_fail_threshold", 80)
        self.email_enabled    = self.alert_cfg.get("email_enabled", False)
        self.log_dir          = self.alert_cfg.get("log_dir", "reports/alerts")
        Path(self.log_dir).mkdir(parents=True, exist_ok=True)

    def should_alert(self, score: float, failed_checks: int) -> bool:
        """Return True if an alert should be raised."""
        return score < self.score_threshold or failed_checks > 0

    def format_email_body(self, table: str, score: dict,
                          results_df: pd.DataFrame) -> str:
        """Build an HTML email body summarising failures."""
        failures = results_df[results_df["status"] == "FAIL"]
        warnings = results_df[results_df["status"] == "WARN"]

        rows_fail = "".join(
            f"<tr style='background:#FCEBEB'>"
            f"<td>{r.check_name}</td><td>{r.column}</td>"
            f"<td>{r.records_failed:,}</td><td>{r.failure_rate_pct}%</td>"
            f"<td>{r.details}</td></tr>"
            for _, r in failures.iterrows()
        )
        rows_warn = "".join(
            f"<tr style='background:#FAEEDA'>"
            f"<td>{r.check_name}</td><td>{r.column}</td>"
            f"<td>{r.records_failed:,}</td><td>{r.failure_rate_pct}%</td>"
            f"<td>{r.details}</td></tr>"
            for _, r in warnings.iterrows()
        )

        return f"""
        <html><body style='font-family:Arial,sans-serif;'>
        <h2>Data Quality Alert — {table}</h2>
        <p>Run time: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}</p>
        <table border='1' cellpadding='6' style='border-collapse:collapse;width:100%'>
          <tr><th>Metric</th><th>Value</th></tr>
          <tr><td>Overall Score</td><td><b>{score['overall_score']}/100 (Grade {score['grade']})</b></td></tr>
          <tr><td>Total Checks</td><td>{score['total_checks']}</td></tr>
          <tr><td style='color:green'>Passed</td><td>{score['passed']}</td></tr>
          <tr><td style='color:orange'>Warned</td><td>{score['warned']}</td></tr>
          <tr><td style='color:red'>Failed</td><td>{score['failed']}</td></tr>
        </table>

        <h3 style='color:red'>Failed Checks</h3>
        <table border='1' cellpadding='6' style='border-collapse:collapse;width:100%'>
          <tr><th>Check</th><th>Column</th><th>Records Failed</th><th>Failure %</th><th>Details</th></tr>
          {rows_fail if rows_fail else "<tr><td colspan='5'>None</td></tr>"}
        </table>

        <h3 style='color:orange'>Warnings</h3>
        <table border='1' cellpadding='6' style='border-collapse:collapse;width:100%'>
          <tr><th>Check</th><th>Column</th><th>Records Failed</th><th>Failure %</th><th>Details</th></tr>
          {rows_warn if rows_warn else "<tr><td colspan='5'>None</td></tr>"}
        </table>
        </body></html>
        """

    def send_email(self, subject: str, html_body: str) -> None:
        """Send an HTML email via SMTP."""
        if not self.email_enabled:
            logger.info("Email alerts disabled — skipping send")
            return

        smtp_cfg = self.alert_cfg.get("smtp", {})
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = smtp_cfg.get("sender")
        msg["To"]      = ", ".join(smtp_cfg.get("recipients", []))
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(smtp_cfg["host"], smtp_cfg.get("port", 587)) as server:
            server.starttls()
            server.login(smtp_cfg["user"], smtp_cfg["password"])
            server.send_message(msg)
        logger.info(f"Alert email sent to {msg['To']}")

    def log_alert(self, table: str, score: dict,
                  results_df: pd.DataFrame) -> None:
        """Write alert details to a JSON log file."""
        failures = results_df[results_df["status"].isin(["FAIL", "WARN"])].to_dict("records")
        alert = {
            "timestamp":  datetime.utcnow().isoformat(),
            "table":      table,
            "score":      score,
            "failures":   failures,
        }
        fname = f"{self.log_dir}/alert_{table}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
        with open(fname, "w") as f:
            json.dump(alert, f, indent=2, default=str)
        logger.info(f"Alert logged to {fname}")

    def raise_alert(self, table: str, score: dict,
                    results_df: pd.DataFrame) -> None:
        """Trigger all configured alert channels."""
        if not self.should_alert(score["overall_score"], score["failed"]):
            logger.info(f"No alert needed for '{table}' (score={score['overall_score']})")
            return

        logger.warning(
            f"ALERT: '{table}' quality score={score['overall_score']}/100 "
            f"({score['failed']} FAILs, {score['warned']} WARNs)"
        )
        self.log_alert(table, score, results_df)

        if self.email_enabled:
            subject  = f"[DQ ALERT] {table} — Score {score['overall_score']}/100 (Grade {score['grade']})"
            body     = self.format_email_body(table, score, results_df)
            self.send_email(subject, body)


if __name__ == "__main__":
    import sys
    sys.path.append(".")
    from checks.quality_checks  import run_all_checks, QualityConfig
    from checks.quality_score   import compute_quality_score

    sample = pd.DataFrame({
        "claim_id":     ["C001", "C002", "C001"],
        "member_id":    ["M1", None, "M3"],
        "claim_status": ["approved", "INVALID", "approved"],
        "billed_amount":[1500, -99, 200],
    })
    config  = QualityConfig(
        table_name       = "claims_test",
        required_columns = ["claim_id", "member_id", "claim_status"],
        not_null_columns = ["claim_id", "member_id"],
        unique_columns   = ["claim_id"],
        range_checks     = {"billed_amount": (0, 50000)},
        allowed_values   = {"claim_status": ["approved", "denied", "pending"]},
    )
    results = run_all_checks(sample, config)
    score   = compute_quality_score(results)

    mgr = AlertManager.__new__(AlertManager)
    mgr.score_threshold = 90
    mgr.email_enabled   = False
    mgr.log_dir         = "reports/alerts"
    Path(mgr.log_dir).mkdir(parents=True, exist_ok=True)
    mgr.raise_alert("claims_test", score, results)
    print("Alert raised — check reports/alerts/")
