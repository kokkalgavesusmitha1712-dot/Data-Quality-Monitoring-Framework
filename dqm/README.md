# ✅ Automated Data Quality Monitoring Framework

> **A reusable, configurable framework that runs automated quality checks on any dataset, computes a weighted data quality score, detects anomalies, triggers alerts on failures, and tracks score trends over time via Power BI / Tableau dashboards — orchestrated daily by Apache Airflow.**

<p>
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white" />
  <img src="https://img.shields.io/badge/SQL-4479A1?style=flat&logo=postgresql&logoColor=white" />
  <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=flat&logo=apacheairflow&logoColor=white" />
  <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=flat&logo=postgresql&logoColor=white" />
  <img src="https://img.shields.io/badge/Azure-0078D4?style=flat&logo=microsoftazure&logoColor=white" />
  <img src="https://img.shields.io/badge/Power%20BI-F2C811?style=flat&logo=powerbi&logoColor=black" />
  <img src="https://img.shields.io/badge/Pandas-150458?style=flat&logo=pandas&logoColor=white" />
  <img src="https://img.shields.io/badge/Pytest-0A9EDC?style=flat&logo=pytest&logoColor=white" />
</p>

---

## 📌 Overview

Poor data quality costs organisations time, trust, and money. This framework automates the detection of data quality issues across any table or dataset — no manual SQL queries needed.

**The problem it solves:** Data teams spend hours manually checking for nulls, duplicates, and invalid values before every report run. This framework eliminates that work entirely by running checks automatically on a schedule, logging results, and alerting the right people when something is wrong.

**What it delivers:**

- **6 check types** — schema, nulls, duplicates, ranges, allowed values, regex patterns
- **Weighted quality score (0–100)** per dataset with a letter grade (A–F)
- **Score history tracking** — see quality trends over days, weeks, months
- **Automated alerting** — email + JSON logs when scores drop below threshold
- **Apache Airflow DAG** — runs all checks daily at 6 AM, parallel per table
- **Executive dashboard** — Power BI / Tableau ready outputs
- **Fully reusable** — apply to any new table in under 5 minutes

---

## 🏗️ Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                    APACHE AIRFLOW (6 AM daily)                │
│                                                               │
│    ┌─────────────┐   ┌─────────────┐   ┌──────────────┐     │
│    │check_claims │   │check_members│   │check_revenue │     │
│    └──────┬──────┘   └──────┬──────┘   └──────┬───────┘     │
│           └─────────────────┼─────────────────┘             │
│                             ▼                                 │
│                   ┌──────────────────┐                        │
│                   │ generate_report  │                        │
│                   └──────────────────┘                        │
└───────────────────────────────────────────────────────────────┘
                             │
       ┌──────────────────────┼───────────────────────┐
       ▼                      ▼                        ▼
┌─────────────┐      ┌────────────────┐     ┌────────────────┐
│   EXTRACT   │      │  QUALITY CHECKS│     │    OUTPUTS     │
│  SQL / CSV  │  →   │ schema         │  →  │ Score History  │
│  Azure Blob │      │ nulls          │     │ Alert Logs     │
│  PostgreSQL │      │ duplicates     │     │ CSV Reports    │
└─────────────┘      │ ranges         │     │ Power BI       │
                     │ allowed values │     │ Tableau        │
                     │ regex          │     └────────────────┘
                     └────────────────┘
                             │
                             ▼
                    ┌────────────────┐
                    │  QUALITY SCORE │
                    │  0 – 100 / A–F │
                    │  Alert if < 90 │
                    └────────────────┘
```

---

## ✨ Key Features

- **6 quality check types** — schema validation, null checks, duplicate detection, range validation, allowed-value enforcement, regex pattern matching
- **Weighted scoring engine** — produces a single 0–100 quality score with a letter grade per table run
- **Score history & trending** — appends every run to a history CSV so you can track quality over time
- **Configurable thresholds** — set WARN and FAIL thresholds per check type via a simple config object
- **Apache Airflow DAG** — parallel daily checks with XCom-based result passing and consolidated report
- **Multi-channel alerting** — email (SMTP) + JSON alert logs with full failure detail
- **14 unit tests** — full pytest suite covering every check type and the scoring engine
- **SQL-native checks** — 8 SQL queries for a SQL-layer DQ approach alongside the Python framework
- **Reusable for any table** — add a new `QualityConfig` object and it's monitored in minutes

---

## 🗂️ Project Structure

```
data-quality-monitoring/
│
├── 📁 checks/
│   ├── quality_checks.py       # Core: 6 check types + CheckResult + QualityConfig
│   └── quality_score.py        # Scoring engine: 0–100 score, grade, history tracking
│
├── 📁 alerts/
│   └── alert_manager.py        # Email + JSON alerting when scores fail
│
├── 📁 reports/
│   └── report_generator.py     # Per-table + multi-table summary report generator
│
├── 📁 pipelines/
│   ├── extract/
│   │   └── extract_data.py     # SQL + CSV data extraction with sample fallback
│   └── load/                   # (extend: write scores to warehouse)
│
├── 📁 airflow/
│   └── dags/
│       └── dq_monitoring_dag.py # Airflow DAG: daily parallel checks + report
│
├── 📁 sql/
│   └── checks/
│       └── dq_check_queries.sql # 8 SQL-native quality check queries
│
├── 📁 notebooks/
│   ├── 01_check_exploration.ipynb   # Explore check results interactively
│   └── 02_score_trend_analysis.ipynb # Visualise quality score trends
│
├── 📁 dashboards/
│   ├── powerbi/                # Power BI (.pbix) quality dashboard
│   └── tableau/                # Tableau (.twbx) quality dashboard
│
├── 📁 data/
│   ├── processed/
│   │   └── quality_score_history.csv  # Auto-generated score history
│   └── sample/
│       ├── claims_sample.csv          # 300-row sample with intentional issues
│       ├── members_sample.csv         # 200-row member sample
│       └── generate_sample_data.py    # Regenerate sample data
│
├── 📁 reports/
│   └── alerts/                 # Auto-generated JSON alert logs
│
├── 📁 tests/
│   └── test_quality_checks.py  # 14 pytest unit tests
│
├── 📁 config/
│   └── config.yaml             # DB, alert, and threshold config template
│
├── run_pipeline.py             # Main entry point — run all checks end-to-end
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 📊 Quality Check Types

| Check | What It Tests | Status Levels |
|-------|--------------|---------------|
| Schema check | All required columns exist | PASS / FAIL |
| Null check | Null rate per column vs warn/fail thresholds | PASS / WARN / FAIL |
| Duplicate check | Duplicate values in key columns | PASS / WARN / FAIL |
| Range check | Numeric columns within min/max bounds | PASS / FAIL |
| Allowed values | Categorical columns contain only valid values | PASS / FAIL |
| Regex check | String columns match expected format | PASS / FAIL |

---

## 📐 Quality Scoring Weights

| Dimension | Weight |
|-----------|--------|
| Null check | 25% |
| Schema check | 20% |
| Duplicate check | 20% |
| Range check | 15% |
| Allowed values | 10% |
| Regex check | 10% |

**Grade scale:** A ≥ 95 · B ≥ 85 · C ≥ 70 · D ≥ 55 · F < 55

---

## ⚙️ Setup & Installation

### 1. Clone the repository

```bash
git clone https://github.com/sushmitha-kokkalgave/data-quality-monitoring.git
cd data-quality-monitoring
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Generate sample data

```bash
python data/sample/generate_sample_data.py
```

### 4. Run the full pipeline (no DB needed)

```bash
python run_pipeline.py
```

### 5. Run unit tests

```bash
pytest tests/ -v
```

### 6. Plug in your own table (5 minutes)

```python
from checks.quality_checks import QualityConfig, run_all_checks
from checks.quality_score  import compute_quality_score

config = QualityConfig(
    table_name       = "your_table",
    required_columns = ["id", "name", "status"],
    not_null_columns = ["id", "name"],
    unique_columns   = ["id"],
    range_checks     = {"amount": (0, 100000)},
    allowed_values   = {"status": ["active", "inactive"]},
)
results = run_all_checks(your_df, config)
score   = compute_quality_score(results)
print(f"Score: {score['overall_score']}/100  Grade: {score['grade']}")
```

---

## 📈 Sample Output

Running on the included sample claims data (300 rows with intentional issues):

```
======================================================
  DATA QUALITY REPORT — CLAIMS
  Run: 2024-12-01 06:00 UTC
======================================================
  Overall Score : 74.5/100  |  Grade: C
  Total Checks  : 12
  Passed        : 8
  Warned        : 1
  Failed        : 3
------------------------------------------------------
  FAILED CHECKS:
    ✗ [duplicate_check]      col=claim_id    failed=2
    ✗ [range_check]          col=billed_amount  failed=3
    ✗ [allowed_values_check] col=claim_status   failed=5
  WARNINGS:
    ! [null_check]           col=member_id   failed=15
======================================================
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| Language | Python 3.8+, SQL |
| Orchestration | Apache Airflow |
| Database | PostgreSQL, Azure SQL |
| Cloud | Azure (configurable) |
| Libraries | Pandas, NumPy, SQLAlchemy |
| Visualization | Power BI, Tableau, Plotly |
| Testing | Pytest (14 unit tests) |
| Alerting | SMTP Email, JSON logs |

---

## 🤝 Contributing

Contributions welcome! Ideas to extend:
- Add a **Slack / Microsoft Teams** webhook to `alert_manager.py`
- Add a **Great Expectations** integration layer
- Add **dbt test** compatibility

---

## 👩‍💻 Author

**Sushmitha Kokkalgave** — Senior Data Analyst  
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/sushmitha-kokkalgave)
[![Email](https://img.shields.io/badge/Email-EA4335?style=flat&logo=gmail&logoColor=white)](mailto:susmitha.data97@gmail.com)
[![GitHub](https://img.shields.io/badge/GitHub-181717?style=flat&logo=github&logoColor=white)](https://github.com/sushmitha-kokkalgave)

---

<p align="center"><i>Because 99.8% data accuracy doesn't happen by accident — it's engineered.</i></p>
