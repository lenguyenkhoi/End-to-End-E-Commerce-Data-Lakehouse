import streamlit as st
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from shared.config import PROJECT_ID, SERVICE_ACCT_KEY

# This file contains helper functions for BigQuery access, including:
# - get_bq_client: Returns a cached BigQuery client for the session
# - run_query: Executes a live query (no cache) and returns a DataFrame
# - run_cached_query: Executes a query with caching (5-minute TTL) for batch/d


@st.cache_resource # Cache the BigQuery client for the session (reused across queries)
def get_bq_client():
    """
    Returns a cached BigQuery client (created once per session).
    Uses service-account JSON key when present, falls back to
    Application Default Credentials (Cloud Run / GKE / local gcloud).
    """
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCT_KEY,
            scopes=["https://www.googleapis.com/auth/bigquery.readonly"],
        )
        return bigquery.Client(project=PROJECT_ID, credentials=creds)
    except FileNotFoundError:
        return bigquery.Client(project=PROJECT_ID)


def run_query(sql: str) -> pd.DataFrame:
    """Live query — no cache. Use for streaming / real-time data."""
    client = get_bq_client()
    try:
        return client.query(sql).to_dataframe()
    except Exception as e:
        st.error(f"BigQuery error: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def run_cached_query(sql: str) -> pd.DataFrame:
    """Cached query — 5-minute TTL. Use for batch / dim tables."""
    return run_query(sql)
