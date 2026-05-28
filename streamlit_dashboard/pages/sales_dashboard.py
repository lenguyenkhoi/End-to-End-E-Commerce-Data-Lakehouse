import time
import datetime
import streamlit as st
import pandas as pd
from shared.bq import run_cached_query
from shared.chart_helpers import *
from shared.queries import *
from shared.config import *
# sidebar controls
st.markdown("""
    <style>
        [data-testid="stSidebarNav"] {
            display: none;
        }
    </style>
""", unsafe_allow_html=True) #ẩn các pagelink

# sidebar controls
with st.sidebar:
    # pagelinks
    st.header("Navigation")
    st.page_link("dashboard.py", label = "🏠 Home")
    st.page_link("pages/event_stream.py",label =  "📈 Real-time Stream")
    st.page_link("pages/sales_dashboard.py", label =  "📊 Sales Dashboard")
    st.page_link("pages/forecasting.py", label =  "🔮 Forecast Dashboard")
    st.page_link("pages/ai_advisor.py" , label = "🤖 AI Advisor")
    st.markdown("---")
    st.header("Dashboard Controls")
    lookback_d = st.slider("Lookback window (days)", 7, 365, 90, key="lookback_d")
    st.markdown("---")
    st.markdown("**Overrides Lookback**")
    # Slicer: Select Year and Quarter (Multiselect)
    selected_years = st.multiselect("Year:", [2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030], default=[])
    selected_quarters = st.multiselect("Quarter:", [1, 2, 3, 4], default=[])
    refresh_s  = 120   # batch refreshes less often
    last_refresh_placeholder = st.empty()
    
# Main Page
st.markdown(CSS, unsafe_allow_html=True)
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
st.set_page_config(page_title="KHOI E-Commerce-Sales Dashboard", layout="wide")
st.header("KHOI-Ecomerce Sales Dashboard")

# KPT cards
st.markdown('<div class="section-badge">KPIs </div>',unsafe_allow_html=True)
kpi_cols  = st.columns(5)
ph_b_kpi  = [c.empty() for c in kpi_cols]
st.markdown("---")

# Monthly charts
st.markdown('<div class="section-badge">Monthly performance </div>',unsafe_allow_html=True)
ph_monthly = st.empty()

st.markdown("---")

# Category/payment
col3, col4, col5  = st.columns([1, 1, 1])
# category revenue
with col3:
    st.markdown('<div class="section-badge">Category revenue </div>',unsafe_allow_html=True)
    ph_category = st.empty()

# Payment method split
with col4:
    st.markdown('<div class="section-badge">Payment split</div>',unsafe_allow_html=True)
    ph_payment  = st.empty()

# profit by location
with col5:
    st.markdown('<div class="section-badge"> Profit by Location</div>', unsafe_allow_html=True)
    ph_location = st.empty()
st.markdown("---")

# Order status funnel
st.markdown('<div class="section-badge">Order status </div>',unsafe_allow_html=True)
ph_status = st.empty()
st.markdown("---")

# Top products 
st.markdown('<div class="section-badge">Top products </div>',unsafe_allow_html=True)
ph_products = st.empty()
st.markdown("---")

# Promo performance
st.markdown('<div class="section-badge">Promo performance </div>',unsafe_allow_html=True)
ph_promo = st.empty()

# BATCH LOOP
_iter = 0
while True:
    now = datetime.datetime.now().strftime("%H:%M:%S")
    _iter += 1

    # 1. Generate dynamic time filters based on lookback and slicer selections
    tf_fs = get_time_filter("fs.date_key", lookback_d, selected_years, selected_quarters)
    tf_fo = get_time_filter("fo.date_key", lookback_d, selected_years, selected_quarters)
    tf_dk = get_time_filter("date_key", lookback_d, selected_years, selected_quarters)

    # 2. Fetch data for all charts
    df_bkpis = run_cached_query(sql_batch_kpis(tf_fs))
    df_monthly= run_cached_query(sql_monthly_revenue(tf_fs))
    df_cat = run_cached_query(sql_category_revenue(tf_dk))
    df_pay = run_cached_query(sql_payment_split(tf_fo))
    df_status = run_cached_query(sql_order_status_funnel(tf_fo))
    df_prods = run_cached_query(sql_top_products(tf_fs))
    df_promo = run_cached_query(sql_promo_performance(tf_fs))
    df_loc = run_cached_query(sql_profit_by_location(tf_fs))
    
    # KPI cards
    if not df_bkpis.empty:
        row = df_bkpis.iloc[0]
        b_kpi_defs = [
            ("Total orders", f"{int(row.get('total_orders',0) or 0):,}", "💰",None),
            ("Total revenue", f"${float(row.get('total_revenue',0) or 0)/1e6:.2f}M", "📊",None),
            ("Avg order value", f"${float(row.get('avg_order_value',0) or 0):,.0f}", "📈",None),
            ("Avg margin", f"{float(row.get('avg_margin_pct',0) or 0):.1f}%", "📉",None),
            ("Unique customers", f"{int(row.get('unique_customers',0) or 0):,}", "👥",None),
        ]
        for ph, (label, val, icon, delta) in zip(ph_b_kpi, b_kpi_defs):
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
        tbl["Margin %"] = tbl["Margin %"].apply(lambda x: f"{x:.1f}%" if pd.notna(x) else "-")
        tbl["Avg rating"] = tbl["Avg rating"].apply(lambda x: f"★ {x:.1f}" if pd.notna(x) else "-")
        ph_products.dataframe(tbl, use_container_width=True, hide_index=True, key=f"products_{_iter}")

    # Promo table
    if not df_promo.empty:
        display_cols_p = {
            "promo_code": "Promo code",
            "discount_percent": "Discount %",
            "orders": "Orders",
            "net_revenue": "Revenue ($)",
            "discount_given": "Discount given ($)",
            "avg_margin": "Avg margin %",
            "is_active": "Active",
        }
        tbl_p = df_promo[list(display_cols_p.keys())].rename(columns=display_cols_p)
        ph_promo.dataframe(tbl_p, use_container_width=True, hide_index=True, key=f"promo_{_iter}")
        
    # profit by location
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