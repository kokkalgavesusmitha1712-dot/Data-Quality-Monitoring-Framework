"""
extract_data.py
Extracts tables from a SQL database or CSV files
for quality check processing.
"""

import pandas as pd
import logging
import yaml
from sqlalchemy import create_engine, text
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SAMPLE_FILES = {
    "claims":          "data/sample/claims_sample.csv",
    "members":         "data/sample/members_sample.csv",
    "revenue_actuals": "data/sample/revenue_sample.csv",
}


def get_engine(config_path: str = "config/config.yaml"):
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    db  = cfg["sources"]["sql_db"]
    url = f"{db['dialect']}://{db['user']}:{db['password']}@{db['host']}:{db['port']}/{db['database']}"
    return create_engine(url)


def extract_table(table_name: str,
                  config_path: str = "config/config.yaml",
                  use_sample: bool = False) -> pd.DataFrame:
    """
    Extract a full table. Falls back to sample CSV if
    use_sample=True or if DB connection is unavailable.
    """
    if use_sample or not Path(config_path).exists():
        sample_path = SAMPLE_FILES.get(table_name)
        if sample_path and Path(sample_path).exists():
            df = pd.read_csv(sample_path)
            logger.info(f"Loaded sample data for '{table_name}': {len(df):,} rows")
            return df
        raise FileNotFoundError(f"No sample data found for table: {table_name}")

    try:
        engine = get_engine(config_path)
        with engine.connect() as conn:
            df = pd.read_sql(text(f"SELECT * FROM {table_name}"), conn)
        logger.info(f"Extracted '{table_name}' from DB: {len(df):,} rows")
        return df
    except Exception as e:
        logger.warning(f"DB extraction failed for '{table_name}': {e}")
        logger.info("Falling back to sample data")
        return extract_table(table_name, use_sample=True)


def extract_incremental(table_name: str, date_col: str,
                         since_date: str,
                         config_path: str = "config/config.yaml") -> pd.DataFrame:
    """Extract only records updated since a given date (incremental load)."""
    engine = get_engine(config_path)
    query  = text(f"SELECT * FROM {table_name} WHERE {date_col} >= :since")
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"since": since_date})
    logger.info(f"Incremental extract '{table_name}' since {since_date}: {len(df):,} rows")
    return df


if __name__ == "__main__":
    df = extract_table("claims", use_sample=True)
    print(df.shape)
    print(df.dtypes)
