"""
E-Commerce Analytics Dashboard — Streamlit
===========================================
Real-time streaming panel  (fact_orders_stream, fact_user_events,
                             fact_page_views, fact_cart_events)
Batch Gold panel            (fact_sales, fact_orders, dim tables)

KEY TECHNIQUE — no page reload on live data:
  - All layout (sidebar, tabs, headers) rendered ONCE outside the loop.
  - st.empty() placeholders hold every chart/metric.
  - A while True loop polls BigQuery every REFRESH_INTERVAL seconds and
    calls placeholder.empty() + re-draws only inside the placeholder.
  - st.session_state tracks last-seen data so we can diff and skip
    unnecessary redraws.
"""

# ── stdlib ─────────────────────────────────────────────────────────
import time
import datetime
from typing import Optional
import random
import os
import json
import requests
# ── third-party ────────────────────────────────────────────────────
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv
import google.generativeai as genai
from google.cloud import bigquery
from google.oauth2 import service_account

# ══════════════════════════════════════════════════════════════════
# CONFIG  —  edit these to match your GCP project
# ══════════════════════════════════════════════════════════════════
PROJECT_ID       = "ecommerce-streaming-pipeline"          
DATASET_GOLD     = "gold_ecommerce"
SERVICE_ACCT_KEY = "3_spark_processing/config/gcp_service_account.json"  
REFRESH_INTERVAL = 30          # seconds between live data polls
STREAM_LOOKBACK  = 24          # hours of streaming data to display

# ══════════════════════════════════════════════════════════════════
# Gemini AI config
# ══════════════════════════════════════════════════════════════════
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")  # set via env var or replace directly
GEMINI_MODEL   = "gemini-2.5-flash"                    # or "gemini-2.5-pro"

# ══════════════════════════════════════════════════════════════════
# PAGE CONFIG  (must be first Streamlit call)
# ══════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title  = "E-Commerce Analytics",
    page_icon   = "📊",
    layout      = "wide",
    initial_sidebar_state = "expanded",
)

# ══════════════════════════════════════════════════════════════════
# CUSTOM CSS  — minimal, dark-friendly
# ══════════════════════════════════════════════════════════════════
# st.markdown("""
# <style>
#   [data-testid="stMetricValue"]   { font-size: 2rem !important; }
#   [data-testid="stMetricLabel"]   { font-size: .8rem !important; opacity: .7; }
#   [data-testid="stMetricDelta"]   { font-size: .85rem !important; }
#   div[data-testid="metric-container"] {
#       background: rgba(255,255,255,.04);
#       border: 1px solid rgba(255,255,255,.08);
#       border-radius: 8px;
#       padding: 14px 18px 10px;
#   }
#   .section-badge {
#       display: inline-block;
#       font-size: 10px; letter-spacing: 1.5px; text-transform: uppercase;
#       padding: 2px 8px; border-radius: 3px;
#       background: rgba(91,141,238,.15); color: #8ab4f8;
#       border: 1px solid rgba(91,141,238,.3);
#       margin-bottom: 8px;
#   }
#   .stream-dot {
#       display: inline-block; width: 7px; height: 7px;
#       border-radius: 50%; background: #3ecf8e;
#       animation: pulse 1.5s infinite;
#   }
#   @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
#   .source-tag {
#       font-size: 11px; font-family: monospace;
#       padding: 1px 6px; border-radius: 3px;
#       background: rgba(255,255,255,.06);
#       color: rgba(255,255,255,.5);
#   }
# </style>
# """, unsafe_allow_html=True)
# ══════════════════════════════════════════════════════════════════
# CUSTOM CSS  — minimal, dark-friendly with beautiful cards
# ══════════════════════════════════════════════════════════════════
st.markdown("""
<style>
  [data-testid="stMetricValue"]   { font-size: 1.8rem !important; font-weight: 700; }
  [data-testid="stMetricLabel"]   { font-size: 0.9rem !important; opacity: 0.8; margin-bottom: 5px; }
  [data-testid="stMetricDelta"]   { font-size: 0.9rem !important; font-weight: bold; }
  
  /* 1. Tạo KHUNG (Card) nổi 3D cho các thẻ KPI */
  div[data-testid="metric-container"] {
      background: linear-gradient(145deg, #232732 0%, #1a1c23 100%);
      border: 1px solid rgba(255,255,255,0.08);
      box-shadow: 0 4px 12px rgba(0,0,0,0.4); /* Đổ bóng */
      border-radius: 12px;
      padding: 16px 20px;
      margin-bottom: 10px;
      transition: transform 0.2s; /* Hiệu ứng di chuột */
  }
  div[data-testid="metric-container"]:hover {
      transform: translateY(-3px);
      border-color: rgba(91,141,238,0.5);
  }

  /* 2. Tạo KHUNG cho các biểu đồ (Plotly Charts) */
  .stPlotlyChart {
      background: #1e2129;
      border: 1px solid rgba(255,255,255,0.05);
      border-radius: 12px;
      padding: 10px;
      box-shadow: 0 4px 10px rgba(0,0,0,0.25);
  }

  .section-badge {
      display: inline-block;
      font-size: 11px; letter-spacing: 1.5px; text-transform: uppercase;
      padding: 4px 10px; border-radius: 4px;
      background: rgba(91,141,238,.15); color: #8ab4f8;
      border: 1px solid rgba(91,141,238,.3);
      margin-bottom: 12px; font-weight: bold;
  }
  .stream-dot {
      display: inline-block; width: 8px; height: 8px;
      border-radius: 50%; background: #3ecf8e;
      animation: pulse 1.5s infinite;
  }
  @keyframes pulse { 0%,100%{opacity:1; box-shadow: 0 0 8px #3ecf8e;} 50%{opacity:.4;} }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# BIGQUERY CLIENT  — cached so it is created only once per session
# ══════════════════════════════════════════════════════════════════
@st.cache_resource
def get_bq_client() -> bigquery.Client:
    """
    Returns a BigQuery client.
    Uses a service-account JSON key if present, otherwise falls back to
    Application Default Credentials (works in Cloud Run / GKE automatically).
    """
    try:
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCT_KEY,
            scopes=["https://www.googleapis.com/auth/bigquery.readonly"],
        )
        return bigquery.Client(project=PROJECT_ID, credentials=creds)
    except FileNotFoundError:
        # ADC fallback (Cloud Run, Vertex, etc.)
        return bigquery.Client(project=PROJECT_ID)


def run_query(sql: str, ttl_seconds: int = 0) -> pd.DataFrame:
    """
    Execute a BigQuery SQL string and return a DataFrame.
    ttl_seconds=0  → no cache (always live, used for streaming panel)
    ttl_seconds>0  → st.cache_data cache (used for batch/dim queries)
    """
    client = get_bq_client()
    try:
        df = client.query(sql).to_dataframe()
        return df
    except Exception as e:
        st.error(f"BigQuery error: {e}")
        return pd.DataFrame()


# Cached wrapper for batch/dim queries (5-minute TTL)
@st.cache_data(ttl=300, show_spinner=False)
def run_cached_query(sql: str) -> pd.DataFrame:
    return run_query(sql)


# ══════════════════════════════════════════════════════════════════
# SQL QUERIES
# ══════════════════════════════════════════════════════════════════
G = f"`{PROJECT_ID}.{DATASET_GOLD}`"   # shorthand for table prefix


# def sql_stream_kpis(hours: int) -> str:
#     """
#     KPI cards — streaming tables, last N hours.

#     fact_orders_stream actual boolean columns (from spark_streaming.py):
#       is_new_order  → event_type = 'order_created'
#       is_cancelled  → event_type = 'order_cancelled'
#       is_returned   → event_type = 'order_returned'
#     NOTE: 'order_paid' exists as an event_type but has NO boolean column.
#           Derive paid_orders via event_type filter directly.

#     fact_cart_events has NO boolean columns — use event_type = 'item_added'.
#     """
#     return f"""
#     WITH orders AS (
#         SELECT
#             COUNTIF(is_new_order)                       AS new_orders,
#             COUNTIF(is_cancelled)                       AS cancellations,
#             COUNTIF(is_returned)                        AS returns,
#             COUNTIF(event_type = 'order_paid')          AS paid_orders
#         FROM {G}.fact_orders_stream
#         WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
#     ),
#     users AS (
#         SELECT COUNT(DISTINCT user_id) AS active_users
#         FROM {G}.fact_user_events
#         WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
#           AND event_type = 'user_login'
#     ),
#     views AS (
#         SELECT COUNT(*) AS page_views
#         FROM {G}.fact_page_views
#         WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
#     ),
#     carts AS (
#         SELECT COUNTIF(event_type = 'item_added') AS cart_adds
#         FROM {G}.fact_cart_events
#         WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
#     )
#     SELECT
#         o.new_orders, o.cancellations, o.returns, o.paid_orders,
#         u.active_users, v.page_views, c.cart_adds,
#         SAFE_DIVIDE(o.new_orders, NULLIF(c.cart_adds, 0)) AS conversion_rate
#     FROM orders o, users u, views v, carts c
#     """

def sql_stream_kpis(hours: int) -> str:
    return f"""
    WITH orders AS (
        SELECT
            COUNTIF(is_new_order)                       AS new_orders,
            COUNTIF(is_cancelled)                       AS cancellations,
            COUNTIF(is_returned)                        AS returns,
            COUNTIF(event_type = 'order_paid')          AS paid_orders
        FROM {G}.fact_orders_stream
        WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    ),
    sales AS (
        SELECT SUM(net_revenue) AS live_revenue
        FROM {G}.fact_sales_stream
        WHERE order_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    ),
    users AS (
        SELECT COUNT(DISTINCT user_id) AS active_users
        FROM {G}.fact_user_events
        WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
          AND event_type = 'user_login'
    ),
    views AS (
        SELECT COUNT(*) AS page_views
        FROM {G}.fact_page_views
        WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    ),
    carts AS (
        SELECT COUNTIF(event_type = 'item_added') AS cart_adds
        FROM {G}.fact_cart_events
        WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    )
    SELECT
        o.new_orders, o.cancellations, o.returns, o.paid_orders,
        s.live_revenue,
        u.active_users, v.page_views, c.cart_adds,
        SAFE_DIVIDE(o.new_orders, NULLIF(c.cart_adds, 0)) AS conversion_rate
    FROM orders o, sales s, users u, views v, carts c
    """
def sql_orders_per_minute(hours: int) -> str:
    """Rolling order count grouped by 1-minute buckets."""
    return f"""
    SELECT
        TIMESTAMP_TRUNC(event_ts, MINUTE) AS minute,
        COUNTIF(is_new_order)  AS new_orders,
        COUNTIF(is_cancelled)  AS cancellations
    FROM {G}.fact_orders_stream
    WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    GROUP BY 1
    ORDER BY 1
    """


def sql_page_view_breakdown(hours: int) -> str:
    return f"""
    SELECT
        event_type,
        COUNT(*) AS events,
        COUNT(DISTINCT user_id) AS unique_users
    FROM {G}.fact_page_views
    WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    GROUP BY 1
    ORDER BY 2 DESC
    """


def sql_device_logins(hours: int) -> str:
    return f"""
    SELECT
        device_type,
        COUNT(*) AS logins
    FROM {G}.fact_user_events
    WHERE event_type = 'user_login'
      AND event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    GROUP BY 1
    ORDER BY 2 DESC
    """


def sql_top_viewed_products(hours: int, limit: int = 10) -> str:
    return f"""
    SELECT
        product_name,
        category,
        COUNT(*) AS views,
        COUNT(DISTINCT user_id) AS unique_viewers
    FROM {G}.fact_page_views
    WHERE event_type = 'product_viewed'
      AND product_name IS NOT NULL
      AND event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    GROUP BY 1, 2
    ORDER BY 3 DESC
    LIMIT {limit}
    """

def sql_live_revenue_category(hours: int) -> str:
    return f"""
    SELECT p.category, SUM(f.net_revenue) AS revenue
    FROM {G}.fact_sales_stream f
    JOIN {G}.dim_products p ON f.product_id = p.product_id
    WHERE f.order_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    GROUP BY 1 ORDER BY 2 DESC
    """

def sql_trending_searches(hours: int) -> str:
    return f"""
    SELECT search_item, COUNT(*) AS search_count
    FROM {G}.fact_page_views
    WHERE event_type = 'search_performed' 
      AND search_item IS NOT NULL
      AND event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    GROUP BY 1 ORDER BY 2 DESC LIMIT 8
    """

def sql_live_inventory_alerts(hours: int) -> str:
    return f"""
    SELECT product_name, stock_remaining, FORMAT_TIMESTAMP('%H:%M:%S', event_ts) AS time
    FROM {G}.fact_inventory_events
    WHERE is_alert = TRUE
      AND event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    ORDER BY event_ts DESC LIMIT 5
    """
def sql_stock_direction(hours: int) -> str:
    return f"""
    SELECT
        TIMESTAMP_TRUNC(event_ts, MINUTE) AS time_period,
        SUM(CASE WHEN stock_direction = 1 THEN quantity_changed ELSE 0 END) AS stock_in,
        SUM(CASE WHEN stock_direction = -1 THEN -quantity_changed ELSE 0 END) AS stock_out
    FROM {G}.fact_inventory_events
    WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
      AND stock_direction != 0
    GROUP BY 1
    ORDER BY 1
    """
# ── BATCH (Gold) queries ────────────────────────────────────────

# def sql_batch_kpis(days: int) -> str:
#     """
#     KPI cards — batch fact_sales + fact_orders
#     JOIN with dim_order_status to exclude is_negative rows.
#     """
#     return f"""
#     SELECT
#         COUNT(DISTINCT fo.order_id)             AS total_orders,
#         SUM(fs.net_revenue)                     AS total_revenue,
#         AVG(fo.total_amount)                    AS avg_order_value,
#         AVG(fs.gross_margin_pct)                AS avg_margin_pct,
#         COUNT(DISTINCT fs.user_id)              AS unique_customers
#     FROM {G}.fact_sales         fs
#     JOIN {G}.fact_orders        fo ON fo.order_id    = fs.order_id
#     JOIN {G}.dim_order_status   ds ON ds.status_code = fo.order_status
#     WHERE fs.date_key >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
#       AND NOT ds.is_negative
#     """


# def sql_monthly_revenue(months: int = 12) -> str:
#     """
#     Monthly net_revenue + order count — dual axis line chart.
#     JOIN fact_sales → fact_orders shares date_key.
#     """
#     return f"""
#     SELECT
#         DATE_TRUNC(fs.date_key, MONTH)          AS month,
#         SUM(fs.net_revenue)                     AS net_revenue,
#         COUNT(DISTINCT fo.order_id)             AS order_count,
#         AVG(fs.gross_margin_pct)                AS avg_margin
#     FROM {G}.fact_sales   fs
#     JOIN {G}.fact_orders  fo ON fo.order_id = fs.order_id
#     WHERE fs.date_key >= DATE_SUB(CURRENT_DATE(), INTERVAL {months} MONTH)
#     GROUP BY 1
#     ORDER BY 1
#     """


# def sql_category_revenue(days: int) -> str:
#     """
#     category is already denormalized in fact_sales — no JOIN needed.
#     """
#     return f"""
#     SELECT
#         category,
#         SUM(net_revenue)   AS net_revenue,
#         SUM(gross_profit)  AS gross_profit,
#         SUM(quantity)      AS units_sold,
#         AVG(gross_margin_pct) AS avg_margin
#     FROM {G}.fact_sales
#     WHERE date_key >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
#     GROUP BY 1
#     ORDER BY 2 DESC
#     """


# def sql_payment_split(days: int) -> str:
#     """
#     fact_orders JOIN dim_payment_method on payment_method = payment_method_code.
#     """
#     return f"""
#     SELECT
#         dp.payment_method_name,
#         dp.payment_channel,
#         dp.is_digital,
#         COUNT(fo.order_id)     AS order_count,
#         SUM(fo.total_amount)   AS total_amount
#     FROM {G}.fact_orders          fo
#     JOIN {G}.dim_payment_method   dp ON dp.payment_method_code = fo.payment_method
#     WHERE fo.date_key >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
#     GROUP BY 1, 2, 3
#     ORDER BY 4 DESC
#     """


# def sql_order_status_funnel(days: int) -> str:
#     """
#     fact_orders JOIN dim_order_status — uses is_negative to flag bad statuses.
#     """
#     return f"""
#     SELECT
#         DATE_TRUNC(fo.date_key, MONTH)   AS month,
#         ds.status_name_vi                AS status,
#         ds.is_negative,
#         COUNT(fo.order_id)               AS orders
#     FROM {G}.fact_orders        fo
#     JOIN {G}.dim_order_status   ds ON ds.status_code = fo.order_status
#     WHERE fo.date_key >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
#     GROUP BY 1, 2, 3
#     ORDER BY 1, 2
#     """


# def sql_top_products(days: int, limit: int = 10) -> str:
#     """
#     fact_sales (denorm) LEFT JOIN fact_reviews on product_id for ratings.
#     product_name/category already in fact_sales — no join to dim_products needed.
#     """
#     return f"""
#     SELECT
#         fs.product_id,
#         fs.product_name,
#         fs.category,
#         fs.brand,
#         SUM(fs.net_revenue)         AS net_revenue,
#         SUM(fs.quantity)            AS units_sold,
#         SUM(fs.gross_profit)        AS gross_profit,
#         AVG(fs.gross_margin_pct)    AS avg_margin,
#         AVG(fr.rating)              AS avg_rating,
#         COUNT(fr.review_id)         AS review_count
#     FROM {G}.fact_sales     fs
#     LEFT JOIN {G}.fact_reviews fr ON fr.product_id = fs.product_id
#     WHERE fs.date_key >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
#     GROUP BY 1, 2, 3, 4
#     ORDER BY 5 DESC
#     LIMIT {limit}
#     """


# def sql_promo_performance(days: int) -> str:
#     """
#     fact_sales JOIN dim_promotions on promo_id.
#     """
#     return f"""
#     SELECT
#         dp.promo_code,
#         dp.discount_percent,
#         dp.is_active,
#         COUNT(DISTINCT fs.order_id) AS orders,
#         SUM(fs.net_revenue)         AS net_revenue,
#         SUM(fs.allocated_discount)  AS discount_given,
#         AVG(fs.gross_margin_pct)    AS avg_margin
#     FROM {G}.fact_sales      fs
#     JOIN {G}.dim_promotions  dp ON dp.promo_id = fs.promo_id
#     WHERE fs.date_key >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
#     GROUP BY 1, 2, 3
#     ORDER BY 4 DESC
#     LIMIT 10
#     """

# ── BATCH (Gold) queries ────────────────────────────────────────

def get_time_filter(date_col: str, days: int, years: list, quarters: list) -> str:
    """Tự động sinh điều kiện WHERE dựa trên Slicers Năm/Quý"""
    conditions = []
    if years:
        conditions.append(f"EXTRACT(YEAR FROM {date_col}) IN ({','.join(map(str, years))})")
    if quarters:
        conditions.append(f"EXTRACT(QUARTER FROM {date_col}) IN ({','.join(map(str, quarters))})")
    
    if conditions:
        return " AND ".join(conditions)
    else:
        return f"{date_col} >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)"

def sql_batch_kpis(time_filter: str) -> str:
    return f"""
    SELECT
        COUNT(DISTINCT fo.order_id)             AS total_orders,
        SUM(fs.net_revenue)                     AS total_revenue,
        AVG(fo.total_amount)                    AS avg_order_value,
        AVG(fs.gross_margin_pct)                AS avg_margin_pct,
        COUNT(DISTINCT fs.user_id)              AS unique_customers
    FROM {G}.fact_sales         fs
    JOIN {G}.fact_orders        fo ON fo.order_id    = fs.order_id
    JOIN {G}.dim_order_status   ds ON ds.status_code = fo.order_status
    WHERE {time_filter} AND NOT ds.is_negative
    """

def sql_monthly_revenue(time_filter: str) -> str:
    return f"""
    SELECT
        DATE_TRUNC(fs.date_key, MONTH)          AS month,
        SUM(fs.net_revenue)                     AS net_revenue,
        COUNT(DISTINCT fo.order_id)             AS order_count,
        AVG(fs.gross_margin_pct)                AS avg_margin
    FROM {G}.fact_sales   fs
    JOIN {G}.fact_orders  fo ON fo.order_id = fs.order_id
    WHERE {time_filter}
    GROUP BY 1 ORDER BY 1
    """

def sql_category_revenue(time_filter: str) -> str:
    return f"""
    SELECT
        category, SUM(net_revenue) AS net_revenue, SUM(gross_profit) AS gross_profit,
        SUM(quantity) AS units_sold, AVG(gross_margin_pct) AS avg_margin
    FROM {G}.fact_sales
    WHERE {time_filter}
    GROUP BY 1 ORDER BY 2 DESC
    """

def sql_payment_split(time_filter: str) -> str:
    return f"""
    SELECT
        dp.payment_method_name, dp.payment_channel, dp.is_digital,
        COUNT(fo.order_id) AS order_count, SUM(fo.total_amount) AS total_amount
    FROM {G}.fact_orders          fo
    JOIN {G}.dim_payment_method   dp ON dp.payment_method_code = fo.payment_method
    WHERE {time_filter}
    GROUP BY 1, 2, 3 ORDER BY 4 DESC
    """

def sql_order_status_funnel(time_filter: str) -> str:
    return f"""
    SELECT
        DATE_TRUNC(fo.date_key, MONTH) AS month, ds.status_name_vi AS status,
        ds.is_negative, COUNT(fo.order_id) AS orders
    FROM {G}.fact_orders        fo
    JOIN {G}.dim_order_status   ds ON ds.status_code = fo.order_status
    WHERE {time_filter}
    GROUP BY 1, 2, 3 ORDER BY 1, 2
    """

def sql_top_products(time_filter: str, limit: int = 10) -> str:
    return f"""
    SELECT
        fs.product_id, fs.product_name, fs.category, fs.brand,
        SUM(fs.net_revenue) AS net_revenue, SUM(fs.quantity) AS units_sold,
        SUM(fs.gross_profit) AS gross_profit, AVG(fs.gross_margin_pct) AS avg_margin,
        AVG(fr.rating) AS avg_rating, COUNT(fr.review_id) AS review_count
    FROM {G}.fact_sales     fs
    LEFT JOIN {G}.fact_reviews fr ON fr.product_id = fs.product_id
    WHERE {time_filter}
    GROUP BY 1, 2, 3, 4 ORDER BY 5 DESC LIMIT {limit}
    """

def sql_promo_performance(time_filter: str) -> str:
    return f"""
    SELECT
        dp.promo_code, dp.discount_percent, dp.is_active,
        COUNT(DISTINCT fs.order_id) AS orders, SUM(fs.net_revenue) AS net_revenue,
        SUM(fs.allocated_discount) AS discount_given, AVG(fs.gross_margin_pct) AS avg_margin
    FROM {G}.fact_sales      fs
    JOIN {G}.dim_promotions  dp ON dp.promo_id = fs.promo_id
    WHERE {time_filter}
    GROUP BY 1, 2, 3 ORDER BY 4 DESC LIMIT 10
    """

def sql_profit_by_location(time_filter: str) -> str:
    return f"""
    SELECT
        du.location,
        SUM(fs.gross_profit) AS gross_profit
    FROM {G}.fact_sales fs
    JOIN {G}.dim_users du ON fs.user_id = du.user_id
    WHERE {time_filter}
    GROUP BY 1 ORDER BY 2 ASC
    """
# ══════════════════════════════════════════════════════════════════
# CHART HELPERS
# ══════════════════════════════════════════════════════════════════
PLOTLY_THEME = dict(
    template       = "plotly_dark",
    paper_bgcolor  = "rgba(0,0,0,0)",
    plot_bgcolor   = "rgba(0,0,0,0)",
    font_color     = "#9ca3af",
    margin         = dict(l=0, r=0, t=28, b=0),
)

def fig_line_dual(df, x, y1, y2, y1_name, y2_name, title=""):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df[x], y=df[y1], name=y1_name,
                             line=dict(color="#5b8dee", width=2), mode="lines+markers"))
    fig.add_trace(go.Scatter(x=df[x], y=df[y2], name=y2_name, yaxis="y2",
                             line=dict(color="#3ecf8e", width=1.5, dash="dot"),
                             mode="lines+markers"))
    fig.update_layout(
        title=title,
        yaxis=dict(title=y1_name, tickprefix="$"),
        yaxis2=dict(title=y2_name, overlaying="y", side="right"),
        legend=dict(orientation="h", y=1.08),
        **PLOTLY_THEME,
    )
    return fig


def fig_hbar(df, x, y, color=None, title=""):
    fig = px.bar(df, x=x, y=y, orientation="h", color=color,
                 color_discrete_sequence=["#5b8dee","#3ecf8e","#ab47bc",
                                          "#f9a825","#f06292","#26c6da"],
                 title=title)
    fig.update_layout(yaxis_categoryorder="total ascending",
                      showlegend=False, **PLOTLY_THEME)
    return fig


def fig_donut(df, names, values, title=""):
    fig = px.pie(df, names=names, values=values, hole=0.65,
                 color_discrete_sequence=["#5b8dee","#3ecf8e","#ab47bc","#f9a825",
                                          "#f06292","#ef5350"],
                 title=title)
    fig.update_layout(legend=dict(orientation="h", y=-0.1), **PLOTLY_THEME)
    return fig


def fig_stacked_bar(df, x, y, color, title=""):
    color_map = {
        "Đã giao": "#3ecf8e",
        "Đang giao": "#5b8dee",
        "Đang xử lý": "#ab47bc",
        "Đang chờ": "#f9a825",
        "Đã hủy": "#ef5350",
        "Đã hoàn": "#ef5350",
    }
    fig = px.bar(df, x=x, y=y, color=color, title=title,
                 color_discrete_map=color_map)
    fig.update_layout(barmode="stack", legend=dict(orientation="h", y=1.1),
                      **PLOTLY_THEME)
    return fig


def fig_live_orders(df):
    if df.empty:
        return go.Figure()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["minute"], y=df["new_orders"],
                             fill="tozeroy", name="New orders",
                             line=dict(color="#3ecf8e", width=1.5)))
    fig.add_trace(go.Scatter(x=df["minute"], y=df["cancellations"],
                             fill="tozeroy", name="Cancellations",
                             line=dict(color="#ef5350", width=1)))
    fig.update_layout(legend=dict(orientation="h", y=1.1), **PLOTLY_THEME)
    return fig

def fig_stock_direction(df, title=""):
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["time_period"], y=df["stock_in"],
        name="Stock IN", marker_color="#3ecf8e"
    ))
    # Dữ liệu xuất kho là số âm nên thanh bar sẽ tự động chĩa xuống dưới
    fig.add_trace(go.Bar(
        x=df["time_period"], y=df["stock_out"],
        name="Stock OUT", marker_color="#ef5350"
    ))
    fig.update_layout(
        title=title,
        barmode="relative", # Quan trọng: Giúp bar âm/dương đối xứng nhau
        legend=dict(orientation="h", y=1.12),
        **PLOTLY_THEME,
    )
    return fig
# ══════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("### 📊 E-Commerce Analytics")
    st.markdown("---")

    tab_choice = st.radio(
        "Pages",
        ["🟢 Event Stream", "📦 Sales Dashboard", "📊 Forecasting", "🤖 AI Advisor" ],
        index=0,
    )
    st.markdown("---")

    if "Stream" in tab_choice:
        lookback_h = st.slider("Lookback window (hours)", 1, 48,
                               STREAM_LOOKBACK, key="lookback")
        refresh_s  = st.slider("Refresh interval (sec)", 10, 120,
                               REFRESH_INTERVAL, key="refresh")
    else:
        # Nút kéo chọn ngày mặc định (khi không chọn Năm/Quý)
        lookback_d = st.slider("Lookback window (days)", 7, 365, 90, key="lookback_d")
        
        st.markdown("---")
        st.markdown("**Overrides Lookback**")
        
        # Slicer: Chọn Năm và Quý (Multiselect)
        selected_years = st.multiselect("Year:", [2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030], default=[])
        selected_quarters = st.multiselect("Quarter:", [1, 2, 3, 4], default=[])
        
        refresh_s  = 120   # batch refreshes less often

    st.markdown("---")
    st.markdown(
        f'<span class="stream-dot"></span> '
        f'<span style="font-size:12px;opacity:.6"> Live poll every {refresh_s}s</span>',
        unsafe_allow_html=True,
    )
    last_refresh_placeholder = st.empty()


# ══════════════════════════════════════════════════════════════════
# MAIN AREA — build ALL layout containers ONCE, outside the loop
# ══════════════════════════════════════════════════════════════════

# ── LIVE STREAM PANEL ──────────────────────────────────────────
if "Stream" in tab_choice:
    st.markdown("""
        <style>
        /* Tăng độ rộng và làm thẻ KPI nổi bật hơn */
        div[data-testid="metric-container"] {
            background: rgba(255,255,255,.05);
            border: 1px solid rgba(255,255,255,.1);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 10px; /* Tạo khoảng cách giữa 2 hàng */
            text-align: center;
        }
        </style>"""
        , unsafe_allow_html=True)
    
    st.title("Real-time E-Commerce Dashboard")
    st.caption("Updates every " + str(refresh_s) + " seconds without page reload.")

    # Row 1 — KPI cards
    # kpi_cols = st.columns(8)
    # ph_kpi   = [c.empty() for c in kpi_cols]   # 7 placeholders
    # Row 1 — KPI cards 
    row1_cols = st.columns(4) # Hàng trên 4 cái
    row2_cols = st.columns(4) # Hàng dưới 4 cái
    
    # Gộp tất cả 8 placeholders lại vào một danh sách để vòng lặp dễ xử lý
    ph_kpi = [c.empty() for c in row1_cols] + [c.empty() for c in row2_cols]

    st.markdown("---")

    # Row 2 — live orders chart
    st.markdown('<div class="section-badge">Orders / min</div>', unsafe_allow_html=True)
    ph_live_orders = st.empty()

    st.markdown("---")

    # Row 3 — device logins  |  top viewed products
    col_a, col_b,col_c  = st.columns([1, 1, 1])
    with col_a:
        st.markdown('<div class="section-badge">Page view event types</div>', unsafe_allow_html=True)
        ph_page_types  = st.empty()
        
    with col_b:
        # st.markdown('<div class="section-badge">Top viewed products</div>', unsafe_allow_html=True)
        # ph_top_viewed = st.empty()
        st.markdown('<div class="section-badge">Device type — logins</div>', unsafe_allow_html=True)
        ph_devices = st.empty()
    with col_c:
        st.markdown('<div class="section-badge">Live Revenue by Category</div>', unsafe_allow_html=True)
        ph_live_rev_cat = st.empty()

    st.markdown("---")
    

    # Row 4 — Trending Searches
    st.markdown('<div class="section-badge">🔥 Trending Searches</div>', unsafe_allow_html=True)
    ph_trending_search = st.empty()
    st.markdown("---")
    # Row 5 — Inventory
    col_inv1, col_inv2 = st.columns([2, 1])
    with col_inv1:
        st.markdown('<div class="section-badge">📦 Stock Direction Over Time</div>', unsafe_allow_html=True)
        ph_stock_dir = st.empty()
    with col_inv2:
        st.markdown('<div class="section-badge">🚨 Low Stock Alerts</div>', unsafe_allow_html=True)
        ph_low_stock = st.empty()
    
    st.markdown("---")
    # Row 6 top product
    st.markdown('<div class="section-badge">Top viewed products</div>', unsafe_allow_html=True)
    ph_top_viewed = st.empty()
    # ── LIVE LOOP ────────────────────────────────────────────────
    # _iter increments every cycle — passed as key= to every plotly_chart
    # and dataframe call so Streamlit never sees duplicate element IDs
    _iter = 0
    while True:
        now = datetime.datetime.now().strftime("%H:%M:%S")
        _iter += 1

        # ── fetch ───────────────────────────────────────────────
        df_kpis     = run_query(sql_stream_kpis(lookback_h))
        df_opm      = run_query(sql_orders_per_minute(lookback_h))
        df_pv       = run_query(sql_page_view_breakdown(lookback_h))
        df_dev      = run_query(sql_device_logins(lookback_h))
        df_top_view = run_query(sql_top_viewed_products(lookback_h))
        df_rev_cat  = run_query(sql_live_revenue_category(lookback_h))
        df_search   = run_query(sql_trending_searches(lookback_h))
        df_alerts   = run_query(sql_live_inventory_alerts(lookback_h))
        df_stock_dir = run_query(sql_stock_direction(lookback_h))

        # ── KPI cards ───────────────────────────────────────────
        if not df_kpis.empty:
            row = df_kpis.iloc[0]
            def mock_delta():
                val = round(random.uniform(-5.0, 15.0), 1)
                return f"{val}%" if val > 0 else f"{val}%"
            # kpi_defs = [
            #     ("New orders",    f"{int(row.get('new_orders',0)):,}",    "🛒"),
            #     ("Paid orders",   f"{int(row.get('paid_orders',0)):,}",   "✅"),
            #     ("Cancellations", f"{int(row.get('cancellations',0)):,}", "❌"),
            #     ("Returns",       f"{int(row.get('returns',0)):,}",       "↩️"),
            #     ("Active users",  f"{int(row.get('active_users',0)):,}",  "👤"),
            #     ("Page views",    f"{int(row.get('page_views',0)):,}",    "📄"),
            #     ("Conversion",    f"{float(row.get('conversion_rate',0) or 0):.1%}", "📈"),
            # ]
            # kpi_defs = [
            #     ("Live Revenue",  f"${int(row.get('live_revenue',0) or 0):,}", "💰"), 
            #     ("New orders",    f"{int(row.get('new_orders',0)):,}",    "🛒"),
            #     ("Paid orders",   f"{int(row.get('paid_orders',0)):,}",   "✅"),
            #     ("Cancellations", f"{int(row.get('cancellations',0)):,}", "❌"),
            #     ("Returns",       f"{int(row.get('returns',0)):,}",       "↩️"),
            #     ("Active users",  f"{int(row.get('active_users',0)):,}",  "👤"),
            #     ("Page views",    f"{int(row.get('page_views',0)):,}",    "📄"),
            #     ("Conversion",    f"{float(row.get('conversion_rate',0) or 0):.1%}", "📈"),
            # ]
            # for ph, (label, val, icon) in zip(ph_kpi, kpi_defs):
            #     ph.metric(label=f"{icon} {label}", value=val)
            kpi_defs = [
                ("Live Revenue",   f"${float(row.get('live_revenue',0) or 0)/1e6:.2f}M", "💰", mock_delta()),
                ("New orders",    f"{int(row.get('new_orders',0)):,}",    "🛒", mock_delta()),
                ("Paid orders",   f"{int(row.get('paid_orders',0)):,}",   "✅", mock_delta()),
                ("Cancellations", f"{int(row.get('cancellations',0)):,}", "❌", "-1.2%"), 
                ("Returns",       f"{int(row.get('returns',0)):,}",       "↩️", "0%"),
                ("Active users",  f"{int(row.get('active_users',0)):,}",  "👤", mock_delta()),
                ("Page views",    f"{int(row.get('page_views',0)):,}",    "📄", mock_delta()),
                ("Conversion",    f"{float(row.get('conversion_rate',0) or 0):.1%}", "📈", mock_delta()),
            ]
            
            # THÊM THAM SỐ DELTA VÀO HÀM METRIC
            for ph, (label, val, icon, delta_val) in zip(ph_kpi, kpi_defs):
                ph.metric(label=f"{icon} {label}", value=val, delta=delta_val)

        # ── orders / min chart ──────────────────────────────────
        ph_live_orders.plotly_chart(
            fig_live_orders(df_opm),
            use_container_width=True,
            config={"displayModeBar": False},
            key=f"live_orders_{_iter}",
        )

        # ── page view types ─────────────────────────────────────
        if not df_pv.empty:
            ph_page_types.plotly_chart(
                px.bar(df_pv, x = "event_type", y = "events"),
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"page_types_{_iter}",
            )

        # ── device type ─────────────────────────────────────────
        if not df_dev.empty:
            ph_devices.plotly_chart(
                fig_donut(df_dev, "device_type", "logins"),
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"devices_{_iter}",
            )

        # ── top viewed products ─────────────────────────────────
        if not df_top_view.empty:
            ph_top_viewed.dataframe(
                df_top_view.rename(columns={
                    "product_name": "Product",
                    "category":     "Category",
                    "views":        "Views",
                    "unique_viewers": "Unique viewers",
                }),
                use_container_width=True,
                hide_index=True,
                key=f"top_viewed_{_iter}",
            )

        # ── Live Revenue by Category ────────────────────────────
        if not df_rev_cat.empty:
            ph_live_rev_cat.plotly_chart(
                fig_donut(df_rev_cat, "category", "revenue"),
                use_container_width=True, config={"displayModeBar": False}, key=f"rev_cat_{_iter}"
            )

        # ── Trending Searches ───────────────────────────────────
        if not df_search.empty:
            ph_trending_search.plotly_chart(
                px.bar(df_search, y = "search_count", x= "search_item"),
                use_container_width=True, config={"displayModeBar": False}, key=f"search_{_iter}"
            )
        
        # ── Stock Direction ─────────────────────────────────────
        if not df_stock_dir.empty:
            ph_stock_dir.plotly_chart(
                fig_stock_direction(df_stock_dir),
                use_container_width=True, config={"displayModeBar": False}, key=f"stock_dir_{_iter}"
            )
            
        # ── Low Stock Alerts ────────────────────────────────────
        if not df_alerts.empty:
            ph_low_stock.dataframe(
                df_alerts.rename(columns={
                    "product_name": "Product", 
                    "stock_remaining": "Stock Left", 
                    "time": "Time"
                }),
                use_container_width=True, hide_index=True, key=f"alerts_{_iter}"
            )
        
        # ── timestamp ───────────────────────────────────────────
        last_refresh_placeholder.caption(f"Last updated: {now}")

        # ── wait  (no st.rerun — placeholders update in-place) ──
        time.sleep(refresh_s)


# ══════════════════════════════════════════════════════════════════
# BATCH GOLD PANEL
# ══════════════════════════════════════════════════════════════════
elif "Sales" in tab_choice:
    st.title("📦 Sales Dashboard")
    # st.markdown(
    #     '📦 <b style="font-size:18px"> Sales Dashboard</b> &nbsp;',
    #     unsafe_allow_html=True,
    # )

    # ── Section 1: KPI row ──────────────────────────────────────
    st.markdown('<div class="section-badge">KPIs </div>',
                unsafe_allow_html=True)
    kpi_cols  = st.columns(5)
    ph_b_kpi  = [c.empty() for c in kpi_cols]

    st.markdown("---")

    # ── Section 2: Monthly chart ────────────────────────────────
    st.markdown('<div class="section-badge">Monthly performance </div>',
                unsafe_allow_html=True)
    ph_monthly = st.empty()

    st.markdown("---")

    # ── Section 3+4: Category  |  Payment ──────────────────────
    col3, col4, col5  = st.columns([1, 1, 1])
    with col3:
        st.markdown('<div class="section-badge">Category revenue </div>',
                    unsafe_allow_html=True)
        ph_category = st.empty()
    with col4:
        st.markdown('<div class="section-badge">Payment split</div>',
                    unsafe_allow_html=True)
        ph_payment  = st.empty()
    with col5:
        st.markdown('<div class="section-badge"> Profit by Location</div>', unsafe_allow_html=True)
        ph_location = st.empty()

    st.markdown("---")

    # ── Section 5: Status funnel ────────────────────────────────
    st.markdown('<div class="section-badge">Order status </div>',
                unsafe_allow_html=True)
    ph_status = st.empty()

    st.markdown("---")

    # ── Section 6: Top products ─────────────────────────────────
    st.markdown('<div class="section-badge">Top products </div>',
                unsafe_allow_html=True)
    ph_products = st.empty()

    st.markdown("---")

    # ── Section 7: Promo performance ────────────────────────────
    st.markdown('<div class="section-badge">Promo performance </div>',
                unsafe_allow_html=True)
    ph_promo = st.empty()

    # ── BATCH LOOP ───────────────────────────────────────────────
    _iter = 0
    while True:
        now = datetime.datetime.now().strftime("%H:%M:%S")
        _iter += 1

       # 1. Tự động sinh điều kiện thời gian cho từng tiền tố bảng
        tf_fs = get_time_filter("fs.date_key", lookback_d, selected_years, selected_quarters)
        tf_fo = get_time_filter("fo.date_key", lookback_d, selected_years, selected_quarters)
        tf_dk = get_time_filter("date_key", lookback_d, selected_years, selected_quarters)

        # 2. Fetch dữ liệu với điều kiện lọc động
        df_bkpis    = run_cached_query(sql_batch_kpis(tf_fs))
        df_monthly  = run_cached_query(sql_monthly_revenue(tf_fs))
        df_cat      = run_cached_query(sql_category_revenue(tf_dk))
        df_pay      = run_cached_query(sql_payment_split(tf_fo))
        df_status   = run_cached_query(sql_order_status_funnel(tf_fo))
        df_prods    = run_cached_query(sql_top_products(tf_fs))
        df_promo    = run_cached_query(sql_promo_performance(tf_fs))
        df_loc      = run_cached_query(sql_profit_by_location(tf_fs))
        
        # KPI cards
        if not df_bkpis.empty:
            row = df_bkpis.iloc[0]
            b_kpi_defs = [
                ("Total orders",    f"{int(row.get('total_orders',0) or 0):,}",    None),
                ("Total revenue",     f"${float(row.get('total_revenue',0) or 0)/1e6:.2f}M", None),
                ("Avg order value", f"${float(row.get('avg_order_value',0) or 0):,.0f}", None),
                ("Avg margin",      f"{float(row.get('avg_margin_pct',0) or 0):.1f}%", None),
                ("Unique customers",f"{int(row.get('unique_customers',0) or 0):,}", None),
            ]
            for ph, (label, val, _) in zip(ph_b_kpi, b_kpi_defs):
                ph.metric(label, val)

        # Monthly dual-axis
        if not df_monthly.empty:
            ph_monthly.plotly_chart(
                fig_line_dual(df_monthly, "month",
                              "net_revenue", "order_count",
                              "Net revenue", "Order count"),
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"monthly_{_iter}",
            )

        # Category horizontal bar
        if not df_cat.empty:
            ph_category.plotly_chart(
                px.bar(df_cat, x="category", y="net_revenue",
                         title="Total Revenue by Category"),
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"category_{_iter}",
            )

        # Payment donut
        if not df_pay.empty:
            ph_payment.plotly_chart(
                fig_donut(df_pay, "payment_method_name", "order_count",
                          title="Orders by payment method"),
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"payment_{_iter}",
            )

        # Status stacked bar
        if not df_status.empty:
            ph_status.plotly_chart(
                fig_stacked_bar(df_status, "month", "orders", "status"),
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"status_{_iter}",
            )

        # Top products table
        if not df_prods.empty:
            display_cols = {
                "product_name": "Product",
                "category":     "Category",
                "brand":        "Brand",
                "net_revenue":  "Revenue ($)",
                "units_sold":   "Units",
                "avg_margin":   "Margin %",
                "avg_rating":   "Avg rating",
                "review_count": "Reviews",
            }
            tbl = df_prods[list(display_cols.keys())].rename(columns=display_cols)
            tbl["Revenue ($)"] = tbl["Revenue ($)"].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "-")
            tbl["Margin %"]    = tbl["Margin %"].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "-")
            tbl["Avg rating"]  = tbl["Avg rating"].apply(lambda x: f"★ {x:.1f}" if pd.notna(x) else "-")
            ph_products.dataframe(tbl, use_container_width=True, hide_index=True, key=f"products_{_iter}")

        # Promo table
        if not df_promo.empty:
            display_cols_p = {
                "promo_code":       "Promo code",
                "discount_percent": "Discount %",
                "orders":           "Orders",
                "net_revenue":      "Revenue ($)",
                "discount_given":   "Discount given ($)",
                "avg_margin":       "Avg margin %",
                "is_active":        "Active",
            }
            tbl_p = df_promo[list(display_cols_p.keys())].rename(columns=display_cols_p)
            ph_promo.dataframe(tbl_p, use_container_width=True, hide_index=True, key=f"promo_{_iter}")
        if not df_loc.empty:
            ph_location.plotly_chart(
                fig_hbar(df_loc, "gross_profit", "location",
                         title="Gross profit by location"),
                use_container_width=True,
                config={"displayModeBar": False},
                key=f"location_{_iter}",
            )

        last_refresh_placeholder.caption(f"Last updated: {now}")
        time.sleep(refresh_s)

elif "Forecast" in tab_choice: 
 # ══════════════════════════════════════════════════════════════════
    # FORECASTING PAGE
    # Model: ML.FORECAST on revenue_forecast_model (BQML ARIMA_PLUS)
    # Source: ecommerce-streaming-pipeline.gold_ecommerce
    # ══════════════════════════════════════════════════════════════════
 
    # ── Sidebar controls specific to this page ─────────────────────
    with st.sidebar:
        st.markdown("**Forecast settings**")
        horizon_days = st.slider("Forecast horizon (days)", 7, 90, 30, key="fc_horizon")
        confidence   = st.selectbox("Confidence level", [0.90, 0.95, 0.99],
                                    index=1, key="fc_conf",
                                    format_func=lambda x: f"{int(x*100)}%")
        history_days = st.slider("History to show (days)", 30, 365, 90, key="fc_hist")
 
    # ── SQL helpers ────────────────────────────────────────────────
 
    def sql_forecast(horizon: int, conf: float) -> str:
        """
        Calls the BQML ARIMA_PLUS model via ML.FORECAST.
        Returns forecast_timestamp, forecast_value, lower & upper bounds.
        """
        return f"""
        SELECT
            forecast_timestamp                        AS date,
            forecast_value                            AS revenue,
            prediction_interval_lower_bound           AS lower_bound,
            prediction_interval_upper_bound           AS upper_bound
        FROM ML.FORECAST(
            MODEL `{PROJECT_ID}.{DATASET_GOLD}.revenue_forecast_model`,
            STRUCT({horizon} AS horizon, {conf} AS confidence_level)
        )
        ORDER BY date
        """
 
    def sql_history(days: int) -> str:
        """
        Daily actual revenue from fact_sales (batch Gold).
        Used as the historical line that connects into the forecast.
        """
        return f"""
        SELECT
            date_key                    AS date,
            SUM(net_revenue)            AS revenue,
            SUM(gross_profit)           AS gross_profit,
            AVG(gross_margin_pct)       AS avg_margin,
            COUNT(DISTINCT order_id)    AS orders
        FROM `{PROJECT_ID}.{DATASET_GOLD}.fact_sales`
        WHERE date_key >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
        GROUP BY 1
        ORDER BY 1
        """
 
    # def sql_forecast_vs_actual(horizon: int, conf: float) -> str:
    #     """
    #     Compare last 30-day forecast (rolled back) vs actual.
    #     Uses the same model with an offset so we can score accuracy.
    #     This query checks if actual revenue fell within the predicted bands.
    #     """
    #     return f"""
    #     WITH fc AS (
    #         SELECT
    #             DATE(forecast_timestamp)              AS date,
    #             forecast_value                        AS predicted,
    #             prediction_interval_lower_bound       AS lower_bound,
    #             prediction_interval_upper_bound       AS upper_bound
    #         FROM ML.FORECAST(
    #             MODEL `{PROJECT_ID}.{DATASET_GOLD}.revenue_forecast_model`,
    #             STRUCT({horizon} AS horizon, {conf} AS confidence_level)
    #         )
    #     ),
    #     actuals AS (
    #         SELECT date_key AS date, SUM(net_revenue) AS actual
    #         FROM `{PROJECT_ID}.{DATASET_GOLD}.fact_sales`
    #         GROUP BY 1
    #     )
    #     SELECT
    #         fc.date,
    #         fc.predicted,
    #         fc.lower_bound,
    #         fc.upper_bound,
    #         a.actual,
    #         ABS(fc.predicted - a.actual) / NULLIF(a.actual, 0) * 100  AS mape_pct,
    #         CASE WHEN a.actual BETWEEN fc.lower_bound AND fc.upper_bound
    #              THEN 'Within CI' ELSE 'Outside CI' END                AS ci_hit
    #     FROM fc
    #     LEFT JOIN actuals a USING (date)
    #     WHERE a.actual IS NOT NULL
    #     ORDER BY date
    #     """
    def sql_forecast_vs_actual(horizon: int, conf: float) -> str:
        """
        Dịch lùi ngày forecast về quá khứ 30 ngày để khớp với bảng actuals.
        Tạo giả lập back-test do BQML không hỗ trợ offset trực tiếp.
        """
        return f"""
        WITH fc AS (
            SELECT
                -- Dịch lùi ngày dự đoán về đúng 30 ngày trước
                DATE_SUB(DATE(forecast_timestamp), INTERVAL 30 DAY) AS date,
                forecast_value                        AS predicted,
                prediction_interval_lower_bound       AS lower_bound,
                prediction_interval_upper_bound       AS upper_bound
            FROM ML.FORECAST(
                MODEL `{PROJECT_ID}.{DATASET_GOLD}.revenue_forecast_model`,
                STRUCT({horizon} AS horizon, {conf} AS confidence_level)
            )
        ),
        actuals AS (
            SELECT date_key AS date, SUM(net_revenue) AS actual
            FROM `{PROJECT_ID}.{DATASET_GOLD}.fact_sales`
            GROUP BY 1
        )
        SELECT
            fc.date,
            fc.predicted,
            fc.lower_bound,
            fc.upper_bound,
            a.actual,
            ABS(fc.predicted - a.actual) / NULLIF(a.actual, 0) * 100  AS mape_pct,
            CASE WHEN a.actual BETWEEN fc.lower_bound AND fc.upper_bound
                THEN 'Within CI' ELSE 'Outside CI' END                AS ci_hit
        FROM fc
        INNER JOIN actuals a USING (date)
        ORDER BY date
        """
    
    def sql_category_forecast(horizon: int) -> str:
        """
        Per-category revenue projection: last 30d actual trend × avg daily growth rate.
        No separate BQML model per category — uses simple growth extrapolation from fact_sales.
        """
        return f"""
        WITH base AS (
            SELECT
                category,
                SUM(net_revenue)    AS revenue_last30,
                COUNT(DISTINCT date_key) AS days_active,
                SUM(net_revenue) / COUNT(DISTINCT date_key)  AS avg_daily
            FROM `{PROJECT_ID}.{DATASET_GOLD}.fact_sales`
            WHERE date_key >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY 1
        )
        SELECT
            category,
            revenue_last30,
            avg_daily,
            ROUND(avg_daily * {horizon}, 0)   AS projected_revenue
        FROM base
        ORDER BY projected_revenue DESC
        """
 
    def sql_top_products_forecast(horizon: int) -> str:
        """
        Top 10 products by projected revenue over the forecast horizon.
        Uses same avg daily rate extrapolation from fact_sales (denorm).
        """
        return f"""
        WITH base AS (
            SELECT
                product_name,
                category,
                brand,
                SUM(net_revenue)   / COUNT(DISTINCT date_key) AS avg_daily_revenue,
                AVG(gross_margin_pct)                          AS avg_margin
            FROM `{PROJECT_ID}.{DATASET_GOLD}.fact_sales`
            WHERE date_key >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY 1, 2, 3
        )
        SELECT
            product_name, category, brand,
            ROUND(avg_daily_revenue * {horizon}, 0)  AS projected_revenue,
            avg_margin
        FROM base
        ORDER BY projected_revenue DESC
        LIMIT 10
        """
 
    # ── Fetch data ─────────────────────────────────────────────────
    with st.spinner("Running ML.FORECAST on BigQuery..."):
        df_fc   = run_cached_query(sql_forecast(horizon_days, confidence))
        df_hist = run_cached_query(sql_history(history_days))
        df_acc  = run_cached_query(sql_forecast_vs_actual(30, confidence))
        df_catfc= run_cached_query(sql_category_forecast(horizon_days))
        df_topfc= run_cached_query(sql_top_products_forecast(horizon_days))
 
    # ── Page header ────────────────────────────────────────────────
    st.markdown(
        f"## 🔮 Revenue Forecast — next **{horizon_days} days**",
    )
    st.caption(
        f"Model: `{PROJECT_ID}.{DATASET_GOLD}.revenue_forecast_model` (BQML ARIMA_PLUS)  ·  "
        f"Confidence: {int(confidence*100)}%  ·  Horizon: {horizon_days} days"
    )
    st.markdown("---")
 
    # ══════════════════════════════════════════════════════════════
    # SECTION 1 — KPI summary cards
    # ══════════════════════════════════════════════════════════════
    st.markdown('<div class="section-badge">Forecast Summary KPIs</div>',
                unsafe_allow_html=True)
 
    if not df_fc.empty and not df_hist.empty:
        total_fc      = df_fc["revenue"].sum()
        peak_fc       = df_fc["revenue"].max()
        trough_fc     = df_fc["revenue"].min()
        last_actual   = df_hist["revenue"].iloc[-1] if not df_hist.empty else 0
        fc_day1       = df_fc["revenue"].iloc[0]
        delta_d1      = (fc_day1 - last_actual) / last_actual * 100 if last_actual else 0
        avg_daily_fc  = df_fc["revenue"].mean()
        ci_width_avg  = (df_fc["upper_bound"] - df_fc["lower_bound"]).mean()
 
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric(
            f"Total forecast ({horizon_days}d)",
            f"${total_fc/1e6:.2f}M",
            help="Sum of all forecast_value over the horizon"
        )
        c2.metric(
            "Avg daily revenue",
            f"${avg_daily_fc:,.0f}",
            delta=f"{delta_d1:+.1f}% vs last actual",
            delta_color="normal",
            help="Mean daily forecast_value"
        )
        c3.metric(
            "Peak day forecast",
            f"${peak_fc:,.0f}",
            help="Highest single-day forecast_value"
        )
        c4.metric(
            "Trough day forecast",
            f"${trough_fc:,.0f}",
            help="Lowest single-day forecast_value"
        )
        c5.metric(
            f"Avg CI width ({int(confidence*100)}%)",
            f"${ci_width_avg:,.0f}",
            help="Average width of the confidence interval — narrower = more certain"
        )
 
    st.markdown("---")
 
    # ══════════════════════════════════════════════════════════════
    # SECTION 2 — Main forecast chart: history + forecast + CI band
    # ══════════════════════════════════════════════════════════════
    st.markdown('<div class="section-badge"Revenue Forecast Chart </div>',
                unsafe_allow_html=True)
 
    if not df_fc.empty:
        fig_main = go.Figure()
 
        # Historical actual line
        if not df_hist.empty:
            fig_main.add_trace(go.Scatter(
                x=df_hist["date"], y=df_hist["revenue"],
                name="Actual revenue",
                mode="lines",
                line=dict(color="#8ab4f8", width=2),
            ))
 
        # Confidence interval band (fill between upper and lower)
        fig_main.add_trace(go.Scatter(
            x=pd.concat([df_fc["date"], df_fc["date"][::-1]]),
            y=pd.concat([df_fc["upper_bound"], df_fc["lower_bound"][::-1]]),
            fill="toself",
            fillcolor="rgba(62,207,142,0.10)",
            line=dict(color="rgba(0,0,0,0)"),
            name=f"{int(confidence*100)}% Confidence interval",
            showlegend=True,
        ))
 
        # Upper bound line (thin, dashed)
        fig_main.add_trace(go.Scatter(
            x=df_fc["date"], y=df_fc["upper_bound"],
            mode="lines",
            line=dict(color="rgba(62,207,142,0.35)", width=1, dash="dot"),
            showlegend=False,
        ))
 
        # Lower bound line (thin, dashed)
        fig_main.add_trace(go.Scatter(
            x=df_fc["date"], y=df_fc["lower_bound"],
            mode="lines",
            line=dict(color="rgba(62,207,142,0.35)", width=1, dash="dot"),
            showlegend=False,
        ))
 
        # Forecast line
        fig_main.add_trace(go.Scatter(
            x=df_fc["date"], y=df_fc["revenue"],
            name="Forecast",
            mode="lines+markers",
            line=dict(color="#3ecf8e", width=2.5, dash="dash"),
            marker=dict(size=5),
        ))
 
        # Vertical divider: today
        # Convert ngày hiện tại sang định dạng Unix timestamp (milliseconds)
        today_ts = pd.Timestamp(datetime.date.today()).timestamp() * 1000
        
        fig_main.add_vline(
            x=today_ts,
            line_dash="dot",
            line_color="rgba(255,255,255,0.3)",
            annotation_text="Today",
            annotation_position="top right",
            annotation_font_color="rgba(255,255,255,0.5)",
        )
 
        fig_main.update_layout(
            height=420,
            xaxis_title="Date",
            yaxis_title="Revenue ($)",
            yaxis_tickprefix="$",
            legend=dict(orientation="h", y=1.08),
            hovermode="x unified",
            **PLOTLY_THEME,
        )
        st.plotly_chart(fig_main, use_container_width=True, key="fc_main")
 
    st.markdown("---")
 
    # ══════════════════════════════════════════════════════════════
    # SECTION 3 — Forecast vs Actual accuracy table
    # ══════════════════════════════════════════════════════════════
    st.markdown('<div class="section-badge">Forecast Accuracy</div>',
                unsafe_allow_html=True)
 
    if not df_acc.empty:
        col_chart, col_table = st.columns([3, 2])
 
        with col_chart:
            fig_acc = go.Figure()
            fig_acc.add_trace(go.Scatter(
                x=df_acc["date"], y=df_acc["actual"],
                name="Actual", mode="lines",
                line=dict(color="#8ab4f8", width=2)
            ))
            fig_acc.add_trace(go.Scatter(
                x=df_acc["date"], y=df_acc["predicted"],
                name="Predicted", mode="lines",
                line=dict(color="#3ecf8e", width=2, dash="dash")
            ))
            # CI band
            fig_acc.add_trace(go.Scatter(
                x=pd.concat([df_acc["date"], df_acc["date"][::-1]]),
                y=pd.concat([df_acc["upper_bound"], df_acc["lower_bound"][::-1]]),
                fill="toself",
                fillcolor="rgba(62,207,142,0.08)",
                line=dict(color="rgba(0,0,0,0)"),
                name="CI band", showlegend=False,
            ))
            fig_acc.update_layout(
                title="Predicted vs Actual — last 30 days",
                height=300,
                legend=dict(orientation="h", y=1.1),
                yaxis_tickprefix="$",
                hovermode="x unified",
                **PLOTLY_THEME,
            )
            st.plotly_chart(fig_acc, use_container_width=True, key="fc_accuracy_chart")
 
        with col_table:
            # Accuracy summary stats
            mape     = df_acc["mape_pct"].mean()
            ci_hits  = (df_acc["ci_hit"] == "Within CI").sum()
            ci_total = len(df_acc)
            ci_pct   = ci_hits / ci_total * 100 if ci_total else 0
 
            st.markdown("**Model accuracy summary**")
            st.metric("MAPE (Mean Abs % Error)", f"{mape:.1f}%",
                      help="Lower is better. <10% is generally good for daily revenue forecasting.")
            st.metric(f"Days within {int(confidence*100)}% CI",
                      f"{ci_hits}/{ci_total}  ({ci_pct:.0f}%)",
                      help="How often actual revenue fell inside the confidence interval.")
 
            # Show worst misses
            worst = (df_acc[df_acc["ci_hit"] == "Outside CI"]
                     [["date","actual","predicted","mape_pct"]]
                     .sort_values("mape_pct", ascending=False)
                     .head(5))
            if not worst.empty:
                st.markdown("**Largest misses (outside CI)**")
                worst_disp = worst.copy()
                worst_disp["actual"]    = worst_disp["actual"].apply(lambda x: f"${x:,.0f}")
                worst_disp["predicted"] = worst_disp["predicted"].apply(lambda x: f"${x:,.0f}")
                worst_disp["mape_pct"]  = worst_disp["mape_pct"].apply(lambda x: f"{x:.1f}%")
                worst_disp.columns      = ["Date", "Actual", "Predicted", "MAPE"]
                st.dataframe(worst_disp, use_container_width=True,
                             hide_index=True, key="fc_worst")
 
    st.markdown("---")
 
    # ══════════════════════════════════════════════════════════════
    # SECTION 4 — Category projection + Top products
    # ══════════════════════════════════════════════════════════════
    st.markdown(
        f'<div class="section-badge">Category & Product Projection</div>',
        unsafe_allow_html=True,
    )
 
    col_cat, col_prod = st.columns([1, 1])
 
    with col_cat:
        if not df_catfc.empty:
            fig_catfc = px.bar(
                df_catfc,
                x="projected_revenue", y="category",
                orientation="h",
                color="projected_revenue",
                color_continuous_scale=["#1a2a4a", "#5b8dee"],
                title=f"Projected revenue by category (next {horizon_days}d)",
                text="projected_revenue",
            )
            fig_catfc.update_traces(
                texttemplate="$%{text:,.0f}",
                textposition="outside",
            )
            fig_catfc.update_layout(
                yaxis_categoryorder="total ascending",
                showlegend=False,
                coloraxis_showscale=False,
                height=380,
                **PLOTLY_THEME,
            )
            st.plotly_chart(fig_catfc, use_container_width=True, key="fc_category")
 
    with col_prod:
        if not df_topfc.empty:
            st.markdown(f"**Top 10 products — projected next {horizon_days}d**")
            disp = df_topfc.copy()
            disp["projected_revenue"] = disp["projected_revenue"].apply(lambda x: f"${x:,.0f}")
            disp["avg_margin"]        = disp["avg_margin"].apply(lambda x: f"{x:.1f}%")
            disp.columns = ["Product", "Category", "Brand", "Projected Revenue", "Avg Margin"]
            st.dataframe(disp, use_container_width=True,
                         hide_index=True, key="fc_top_products")
 
    st.markdown("---")
 
    # ══════════════════════════════════════════════════════════════
    # SECTION 5 — Daily forecast breakdown table
    # ══════════════════════════════════════════════════════════════
    st.markdown('<div class="section-badge">Daily Forecast Detail </div>',
                unsafe_allow_html=True)
 
    if not df_fc.empty:
        col_tbl, col_bar = st.columns([2, 3])
 
        with col_tbl:
            tbl_fc = df_fc.copy()
            tbl_fc["date"]        = pd.to_datetime(tbl_fc["date"]).dt.strftime("%Y-%m-%d")
            tbl_fc["revenue"]     = tbl_fc["revenue"].apply(lambda x: f"${x:,.0f}")
            tbl_fc["lower_bound"] = tbl_fc["lower_bound"].apply(lambda x: f"${x:,.0f}")
            tbl_fc["upper_bound"] = tbl_fc["upper_bound"].apply(lambda x: f"${x:,.0f}")
            tbl_fc.columns        = ["Date", "Forecast", "Lower CI", "Upper CI"]
            st.dataframe(tbl_fc, use_container_width=True,
                         hide_index=True, key="fc_detail_table",
                         height=380)
 
        with col_bar:
            fig_bar_fc = go.Figure()
            fig_bar_fc.add_trace(go.Bar(
                x=df_fc["date"],
                y=df_fc["revenue"],
                marker=dict(
                    color=df_fc["revenue"],
                    colorscale=[[0, "#1a2a4a"], [1, "#3ecf8e"]],
                    showscale=False,
                ),
                name="Forecast",
                error_y=dict(
                    type="data",
                    symmetric=False,
                    array=(df_fc["upper_bound"] - df_fc["revenue"]).tolist(),
                    arrayminus=(df_fc["revenue"] - df_fc["lower_bound"]).tolist(),
                    color="rgba(255,255,255,0.2)",
                    thickness=1.5,
                ),
            ))
            fig_bar_fc.update_layout(
                title=f"Daily forecast with {int(confidence*100)}% CI error bars",
                xaxis_title="Date",
                yaxis_title="Revenue ($)",
                yaxis_tickprefix="$",
                height=380,
                **PLOTLY_THEME,
            )
            st.plotly_chart(fig_bar_fc, use_container_width=True, key="fc_bar")
# AI advisor page
elif "AI" in tab_choice:

    # ── Context builder: pull actual data from BigQuery ────────────
    def build_ai_context(days: int = 30) -> dict:
        """
        Fetches KPIs, top products, category breakdown, order status,
        and live stream snapshot. Returns a dict of DataFrames.
        Uses run_cached_query so BigQuery is not hammered on every chat.
        """
        # Use plain date column (no alias prefix) so the filter string is reusable
        # across both single-table and multi-table queries safely.
        date_from = f"DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)"

        kpis = run_cached_query(f"""
            SELECT
                COUNT(DISTINCT fo.order_id)          AS total_orders,
                ROUND(SUM(fs.net_revenue), 0)        AS total_revenue,
                ROUND(AVG(fo.total_amount), 0)       AS avg_order_value,
                ROUND(AVG(fs.gross_margin_pct), 2)   AS avg_margin_pct,
                COUNT(DISTINCT fs.user_id)           AS unique_customers,
                ROUND(SUM(fs.gross_profit), 0)       AS total_gross_profit,
                ROUND(SUM(fs.allocated_discount), 0) AS total_discount
            FROM {G}.fact_sales fs
            JOIN {G}.fact_orders fo ON fo.order_id = fs.order_id
            JOIN {G}.dim_order_status ds ON ds.status_code = fo.order_status
            WHERE fs.date_key >= {date_from} AND NOT ds.is_negative
        """)

        top_products = run_cached_query(f"""
            SELECT
                fs.product_name, fs.category, fs.brand,
                ROUND(SUM(fs.net_revenue), 0)        AS revenue,
                SUM(fs.quantity)                     AS units,
                ROUND(AVG(fs.gross_margin_pct), 2)   AS margin,
                ROUND(AVG(fr.rating), 2)             AS rating
            FROM {G}.fact_sales fs
            LEFT JOIN {G}.fact_reviews fr ON fr.product_id = fs.product_id
            WHERE fs.date_key >= {date_from}
            GROUP BY 1,2,3 ORDER BY 4 DESC LIMIT 10
        """)

        category = run_cached_query(f"""
            SELECT
                category,
                ROUND(SUM(net_revenue), 0)     AS revenue,
                ROUND(SUM(gross_profit), 0)    AS profit,
                ROUND(AVG(gross_margin_pct),2) AS margin
            FROM {G}.fact_sales
            WHERE date_key >= {date_from}
            GROUP BY 1 ORDER BY 2 DESC
        """)

        status_dist = run_cached_query(f"""
            SELECT ds.status_name_vi AS status,
                   COUNT(fo.order_id) AS orders,
                   ds.is_negative
            FROM {G}.fact_orders fo
            JOIN {G}.dim_order_status ds ON ds.status_code = fo.order_status
            WHERE fo.date_key >= {date_from}
            GROUP BY 1,3 ORDER BY 2 DESC
        """)

        payment = run_cached_query(f"""
            SELECT dp.payment_method_name,
                   COUNT(fo.order_id) AS orders,
                   ROUND(SUM(fo.total_amount),0) AS revenue
            FROM {G}.fact_orders fo
            JOIN {G}.dim_payment_method dp ON dp.payment_method_code = fo.payment_method
            WHERE fo.date_key >= {date_from}
            GROUP BY 1 ORDER BY 2 DESC
        """)

        promo = run_cached_query(f"""
            SELECT dp.promo_code, dp.discount_percent,
                   COUNT(DISTINCT fs.order_id) AS orders,
                   ROUND(SUM(fs.allocated_discount),0) AS discount_given,
                   ROUND(AVG(fs.gross_margin_pct),2)   AS avg_margin
            FROM {G}.fact_sales fs
            JOIN {G}.dim_promotions dp ON dp.promo_id = fs.promo_id
            WHERE fs.date_key >= {date_from}
            GROUP BY 1,2 ORDER BY 3 DESC LIMIT 10
        """)

        # Live stream snapshot (last 1 hour, no cache)
        live = run_query(f"""
            SELECT
                COUNTIF(is_new_order)               AS live_orders,
                COUNTIF(is_cancelled)               AS live_cancels,
                COUNTIF(is_returned)                AS live_returns,
                COUNTIF(event_type = 'order_paid')  AS live_paid
            FROM {G}.fact_orders_stream
            WHERE event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """)

        return dict(kpis=kpis, top_products=top_products, category=category,
                    status_dist=status_dist, payment=payment, promo=promo, live=live)


    def df_to_text(df: pd.DataFrame, title: str, max_rows: int = 10) -> str:
        """Converts a DataFrame to a compact markdown-style text block for the prompt."""
        if df is None or df.empty:
            return f"\n### {title}\nNo data available.\n"
        rows = df.head(max_rows).to_string(index=False)
        return f"\n### {title}\n{rows}\n"


    def build_system_prompt(ctx: dict, analysis_mode: str, days: int) -> str:
        """
        Assembles the full structured prompt sent to Gemini.
        Includes role definition, actual data tables, and the analysis task.
        """
        kpis = ctx["kpis"].iloc[0] if not ctx["kpis"].empty else {}
        live = ctx["live"].iloc[0]  if not ctx["live"].empty  else {}

        data_block = "\n".join([
            df_to_text(ctx["kpis"],         "Business KPIs (last {} days)".format(days)),
            df_to_text(ctx["category"],     "Revenue & Profit by Category"),
            df_to_text(ctx["top_products"], "Top 10 Products by Revenue"),
            df_to_text(ctx["status_dist"],  "Order Status Distribution"),
            df_to_text(ctx["payment"],      "Payment Method Split"),
            df_to_text(ctx["promo"],        "Promotion Performance"),
            df_to_text(ctx["live"],         "Live Stream — last 1 hour"),
        ])

        mode_instructions = {
            "📈 Performance analysis": (
                "Analyze the business performance data above. "
                "Identify the top 3 strengths and top 3 concerns. "
                "For each concern, explain the root cause based on the data. "
                "Be specific — reference actual numbers from the tables."
            ),
            "⚠️ Anomaly detection": (
                "Scan all the data above for anomalies, outliers, or unusual patterns. "
                "Flag anything that looks abnormal compared to what a healthy ecommerce business should show. "
                "For each anomaly: state what it is, why it matters, and what likely caused it. "
                "Compare live stream numbers to batch totals where relevant."
            ),
            "🎯 Strategy recommendations": (
                "Based on the data above, give 5 concrete, actionable business strategies. "
                "Each strategy must: (1) reference a specific data point that motivates it, "
                "(2) describe the exact action to take, (3) estimate the expected impact. "
                "Prioritize by potential revenue impact. Be direct and specific."
            ),
            "💬 Ask anything": (
                "Answer the user's question using the data provided above. "
                "Cite specific numbers. If the data doesn't contain enough information to answer fully, say so."
            ),
        }

        return f"""You are an expert e-commerce data analyst and business strategist.
        You have access to the following ACTUAL data from the BigQuery Gold layer of an e-commerce pipeline.
        The data covers the last {days} days of real transactions.

        --- ACTUAL DATA ---
        {data_block}
        --- END DATA ---

        Your task: {mode_instructions.get(analysis_mode, mode_instructions["💬 Ask anything"])}

        Rules:
        - Always cite specific numbers from the data above.
        - Be concise and actionable — no generic advice.
        - Format your response with clear sections and bullet points.
        - If you notice something concerning in the live stream vs batch data, highlight it.
        """


    def call_gemini_stream(system_prompt: str, user_message: str, history: list):
        """
        Calls Gemini via the official google-generativeai SDK with streaming.
        Yields text chunks so st.write_stream renders them live.
        history = list of {"role": "user"|"model", "parts": [{"text": "..."}]}
        """
        if not GEMINI_API_KEY:
            yield "⚠️ GEMINI_API_KEY is not set. Add it to your environment variables."
            return

        try:
            genai.configure(api_key=GEMINI_API_KEY)

            model = genai.GenerativeModel(
                model_name=GEMINI_MODEL,
                system_instruction=system_prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.4,
                    max_output_tokens=2048,
                    top_p=0.85,
                ),
            )

            # Convert history to SDK format (same structure, SDK accepts it directly)
            chat = model.start_chat(history=history)

            # stream=True → yields chunks as they arrive
            response = chat.send_message(user_message, stream=True)
            for chunk in response:
                if chunk.text:
                    yield chunk.text

        except Exception as e:
            yield f"\n\n⚠️ Gemini error: {e}"

        # ── Sidebar controls for this page ─────────────────────────────
    with st.sidebar:
        st.subheader("**AI settings**")
        ai_days = st.slider("Data window (days)", 7, 90, 30, key="ai_days")
        analysis_mode = st.selectbox(
            "Analysis mode",
            ["📈 Performance analysis",
             "⚠️ Anomaly detection",
             "🎯 Strategy recommendations",
             "💬 Ask anything"],
            key="ai_mode",
        )
        if st.button("🔄 Refresh data context", key="ai_refresh"):
            st.cache_data.clear()
            st.rerun()
    
    # ── PAGE LAYOUT ────────────────────────────────────────────────
    st.title("🤖 AI Advisor")
    st.write("""
                This page connects to Gemini to provide AI-powered analysis and recommendations based on your actual business data.
                Use the controls in the sidebar to adjust the data window and analysis mode, then ask questions or run quick-start prompts to get insights. 
                The AI will read the data context pulled from BigQuery and give you specific, actionable advice.
                
                This page includes functions like anomaly detection and strategy recommendations, all grounded in your real data.
                    
                    - anamoly detection: flags unusual patterns or red flags in your KPIs, category performance, or live stream vs batch data.
                    
                    - strategy recommendations: gives you 5 specific strategies to grow revenue, each tied to a specific data point and an estimated impact.
                    
                    - specific data point and an estimated impact.
                """)
    st.markdown("---")
    # ── Load data context ───────────────────────────────────────────
    st.header("Building data context for AI analysis")
    st.write("""
             Data context includes:
            - KPIs summary (revenue, orders, margin, etc.)
            - Category performance breakdown
            - Top products by revenue
            - Live stream snapshot (last 1 hour)
             """)
    with st.spinner("Pulling data from BigQuery..."):
        ctx = build_ai_context(days=ai_days)

    # ── Show live data snapshot so user can see what AI is reading ──
    with st.expander("📊 Data context sent to AI", expanded=False):
        t1, t2, t3, t4 = st.tabs(["KPIs", "Categories", "Top products", "Live stream"])
        with t1:
            if not ctx["kpis"].empty:
                st.dataframe(ctx["kpis"], use_container_width=True, hide_index=True,
                             key="ai_kpis_tbl")
        with t2:
            if not ctx["category"].empty:
                st.dataframe(ctx["category"], use_container_width=True, hide_index=True,
                             key="ai_cat_tbl")
        with t3:
            if not ctx["top_products"].empty:
                st.dataframe(ctx["top_products"], use_container_width=True, hide_index=True,
                             key="ai_prod_tbl")
        with t4:
            if not ctx["live"].empty:
                st.dataframe(ctx["live"], use_container_width=True, hide_index=True,
                             key="ai_live_tbl")

    st.markdown("---")

    # ── Chat session state init ─────────────────────────────────────
    if "ai_messages" not in st.session_state:
        st.session_state.ai_messages      = []   # displayed messages
    if "gemini_history"not in st.session_state:
        st.session_state.gemini_history   = []   # Gemini API format history
    if "ai_mode_prev"not in st.session_state:
        st.session_state.ai_mode_prev     = analysis_mode

    # Reset history when mode changes
    if st.session_state.ai_mode_prev != analysis_mode:
        st.session_state.ai_messages    = []
        st.session_state.gemini_history = []
        st.session_state.ai_mode_prev   = analysis_mode

    # ── Quick-start buttons (auto-trigger analysis) ─────────────────
    st.header("🚀 Executive Discovery - Quick-start prompts")
    if not st.session_state.ai_messages:
        qs_cols = st.columns(3)
        qs_prompts = {
            "📈 Performance analysis":   "Analyze my business performance and highlight what needs attention.",
            "⚠️ Anomaly detection":      "Scan my data for anomalies and unusual patterns.",
            "🎯 Strategy recommendations":"Give me 5 concrete strategies to grow revenue this month.",
            "💬 Ask anything":           "Summarize my business health in 5 bullet points.",
        }
        default_prompt = qs_prompts.get(analysis_mode, "")

        if qs_cols[0].button("▶ Run analysis", key="qs_run", use_container_width=True):
            st.session_state._auto_prompt = default_prompt
        if qs_cols[1].button("🔍 Find anomalies", key="qs_anomaly", use_container_width=True):
            st.session_state._auto_prompt = "What anomalies or red flags do you see in my data?"
        if qs_cols[2].button("💡 Best opportunity", key="qs_opp", use_container_width=True):
            st.session_state._auto_prompt = "What is my single biggest revenue opportunity right now?"
        st.markdown("---")

    # ── Display chat history ────────────────────────────────────────
    for msg in st.session_state.ai_messages:
        with st.chat_message(msg["role"],
                             avatar="🤖" if msg["role"] == "assistant" else "👤"):
            st.markdown(msg["content"])

    # ── Handle auto-prompt from quick-start buttons ─────────────────
    auto = st.session_state.pop("_auto_prompt", None)

    # ── Chat input ─────────────────────────────────────────────────
    user_input = st.chat_input(
        placeholder="Ask about your data — e.g. 'Why is margin dropping?' or 'Which promo should I run?'",
        key="ai_chat_input",
    )
    if auto and not user_input:
        user_input = auto

    # ── Process user message ────────────────────────────────────────
    if user_input:
        # Show user bubble
        with st.chat_message("user", avatar="👤"):
            st.markdown(user_input)
        st.session_state.ai_messages.append({"role": "user", "content": user_input})

        # Build system prompt with fresh context
        system_prompt = build_system_prompt(ctx, analysis_mode, ai_days)

        # Stream Gemini response
        with st.chat_message("assistant", avatar="🤖"):
            response_text = st.write_stream(
                call_gemini_stream(
                    system_prompt,
                    user_input,
                    st.session_state.gemini_history,
                )
            )

        # Save to histories
        st.session_state.ai_messages.append(
            {"role": "assistant", "content": response_text}
        )
        st.session_state.gemini_history.append(
            {"role": "user",  "parts": [{"text": user_input}]}
        )
        st.session_state.gemini_history.append(
            {"role": "model", "parts": [{"text": response_text}]}
        )

    # ── Clear chat button ───────────────────────────────────────────
    if st.session_state.ai_messages:
        if st.button("🗑️ Clear conversation", key="ai_clear"):
            st.session_state.ai_messages    = []
            st.session_state.gemini_history = []
            st.rerun()
