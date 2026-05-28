import time
import datetime
import streamlit as st
import random
from shared.bq import run_query
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
    st.header("Navigation")
    st.page_link("dashboard.py", label = "🏠 Home")
    st.page_link("pages/event_stream.py",label =  "📈 Real-time Stream")
    st.page_link("pages/sales_dashboard.py", label =  "📊 Sales Dashboard")
    st.page_link("pages/forecasting.py", label =  "🔮 Forecast Dashboard")
    st.page_link("pages/ai_advisor.py" , label = "🤖 AI Advisor")
    st.markdown("---")
    st.header("Streaming Controls")
    lookback_h = st.slider("Lookback window (hours)", 1, 48,STREAM_LOOKBACK, key="lookback")
    refresh_s  = st.slider("Refresh interval (sec)", 10, 120,REFRESH_INTERVAL, key="refresh")
    st.markdown("----")
    last_refresh = st.empty()


# pages
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
st.set_page_config(page_title="KHOI E-Commerce-Real-time Stream", layout="wide")
st.header("Real-time E-Commerce Dashboard")

# Row 1 — KPI cards 
row1_cols = st.columns(4) 
row2_cols = st.columns(4) 

# create empty placeholders for each KPI metric (8 total)
ph_kpi = [c.empty() for c in row1_cols] + [c.empty() for c in row2_cols]
st.markdown("---")

# Row 2 — live orders chart
st.markdown('<div class="section-badge">Orders / min</div>', unsafe_allow_html=True)
ph_live_orders = st.empty()
st.markdown("---")

# Row 3 — device logins|top viewed products
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
# Live Loop
# _iter increments every cycle — passed as key= to every plotly_chart
# and dataframe call so Streamlit never sees duplicate element IDs
_iter = 0
while True:
    now = datetime.datetime.now().strftime("%H:%M:%S")
    _iter += 1
    # fetch all data for this cycle (each is a separate query to BigQuery)
    df_kpis     = run_query(sql_stream_kpis(lookback_h))
    df_opm      = run_query(sql_orders_per_minute(lookback_h))
    df_pv       = run_query(sql_page_view_breakdown(lookback_h))
    df_dev      = run_query(sql_device_logins(lookback_h))
    df_top_view = run_query(sql_top_viewed_products(lookback_h))
    df_rev_cat  = run_query(sql_live_revenue_category(lookback_h))
    df_search   = run_query(sql_trending_searches(lookback_h))
    df_alerts   = run_query(sql_live_inventory_alerts(lookback_h))
    df_stock_dir = run_query(sql_stock_direction(lookback_h))

    # KPI cards
    if not df_kpis.empty:
        row = df_kpis.iloc[0]
        def mock_delta():
            val = round(random.uniform(-5.0, 15.0), 1)
            return f"{val}%" if val > 0 else f"{val}%"
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
        
        # add delta param to metric
        for ph, (label, val, icon, delta_val) in zip(ph_kpi, kpi_defs):
            ph.metric(label=f"{icon} {label}", value=val, delta=delta_val)

    # orders/min chart
    ph_live_orders.plotly_chart(
        fig_live_orders(df_opm),
        use_container_width=True,
        config={"displayModeBar": False},
        key=f"live_orders_{_iter}",
    )

    # page view types
    if not df_pv.empty:
        ph_page_types.plotly_chart(
            px.bar(df_pv, x = "event_type", y = "events"),
            use_container_width=True,
            config={"displayModeBar": False},
            key=f"page_types_{_iter}",
        )

    # device type
    if not df_dev.empty:
        ph_devices.plotly_chart(
            fig_donut(df_dev, "device_type", "logins"),
            use_container_width=True,
            config={"displayModeBar": False},
            key=f"devices_{_iter}",
        )

    # top viewed products
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

    #  Live Revenue by Category 
    if not df_rev_cat.empty:
        ph_live_rev_cat.plotly_chart(
            fig_donut(df_rev_cat, "category", "revenue"),
            use_container_width=True, config={"displayModeBar": False}, key=f"rev_cat_{_iter}"
        )

    #  Trending Searches 
    if not df_search.empty:
        ph_trending_search.plotly_chart(
            px.bar(df_search, y = "search_count", x= "search_item"),
            use_container_width=True, config={"displayModeBar": False}, key=f"search_{_iter}"
        )
    
    #  Stock Direction
    if not df_stock_dir.empty:
        ph_stock_dir.plotly_chart(
            fig_stock_direction(df_stock_dir),
            use_container_width=True, config={"displayModeBar": False}, key=f"stock_dir_{_iter}"
        )
        
    #  Low Stock Alerts 
    if not df_alerts.empty:
        ph_low_stock.dataframe(
            df_alerts.rename(columns={
                "product_name": "Product", 
                "stock_remaining": "Stock Left", 
                "time": "Time"
            }),
            use_container_width=True, hide_index=True, key=f"alerts_{_iter}"
        )
    
    #  timestamp 
    last_refresh.caption(f"Last updated: {now}")

    #  wait  (no st.rerun — placeholders update in-place) ──
    time.sleep(refresh_s)