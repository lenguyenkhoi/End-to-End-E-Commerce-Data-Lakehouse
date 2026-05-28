import datetime
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from shared.bq import run_cached_query
from shared.config import *
from shared.chart_helpers import *
from shared.queries import *


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
    st.header("Forecasting Controls")
    horizon_days = st.slider("Forecast horizon (days)", 7, 90, 30, key="fc_horizon")
    confidence   = st.selectbox("Confidence level", [0.90, 0.95, 0.99],
                                index=1, key="fc_conf",
                                format_func=lambda x: f"{int(x*100)}%")
    history_days = st.slider("History to show (days)", 30, 365, 90, key="fc_hist")

# page header 
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


st.set_page_config(page_title="KHOI E-Commerce-Forecast Dashboard", layout="wide")
st.header("🔮 Revenue Forecast") 

# fetch data 
with st.spinner("Running ML.FORECAST on BigQuery..."):
    df_fc   = run_cached_query(sql_forecast(horizon_days, confidence))
    df_hist = run_cached_query(sql_history(history_days))
    df_acc  = run_cached_query(sql_forecast_vs_actual(30, confidence))
    df_catfc= run_cached_query(sql_category_forecast(horizon_days))
    df_topfc= run_cached_query(sql_top_products_forecast(horizon_days))
 

# Forecast summary KPIS
st.markdown('<div class="section-badge">Forecast Summary KPIs</div>',unsafe_allow_html=True)
if not df_fc.empty and not df_hist.empty:
    total_fc = df_fc["revenue"].sum() 
    peak_fc = df_fc["revenue"].max()  
    trough_fc = df_fc["revenue"].min() 
    last_actual = df_hist["revenue"].iloc[-1] if not df_hist.empty else 0
    fc_day1 = df_fc["revenue"].iloc[0]
    delta_d1 = (fc_day1 - last_actual) / last_actual * 100 if last_actual else 0
    avg_daily_fc = df_fc["revenue"].mean()
    ci_width_avg = (df_fc["upper_bound"] - df_fc["lower_bound"]).mean()
    # Display KPIs in a row
    c1, c2, c3, c4, c5 = st.columns(5)
    # total forecast
    c1.metric(
        f"Total forecast ({horizon_days}d)",
        f"${total_fc/1e6:.2f}M",
        help="Sum of all forecast_value over the horizon"
    )
    # avg daily forecast
    c2.metric(
        "Avg daily revenue",
        f"${avg_daily_fc:,.0f}",
        delta=f"{delta_d1:+.1f}% vs last actual",
        delta_color="normal",
        help="Mean daily forecast_value"
    )
    # peak day forecast
    c3.metric(
        "Peak day forecast",
        f"${peak_fc:,.0f}",
        help="Highest single-day forecast_value"
    )
    # trough day forecast
    c4.metric(
        "Trough day forecast",
        f"${trough_fc:,.0f}",
        help="Lowest single-day forecast_value"
    )
    # CI width
    c5.metric(
        f"Avg CI width ({int(confidence*100)}%)",
        f"${ci_width_avg:,.0f}",
        help="Average width of the confidence interval — narrower = more certain"
    )

st.markdown("---")

# Main forecast chart: history + forecast + CI band
st.markdown('<div class="section-badge">Revenue Forecast Chart</div>',
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


# Forecast vs Actual accuracy table
st.markdown('<div class="section-badge">Forecast Accuracy</div>',
            unsafe_allow_html=True)

if not df_acc.empty:
    col_chart, col_table = st.columns([3, 2])
    with col_chart:
        # actual vs predicted
        fig_acc = go.Figure()
        # actual
        fig_acc.add_trace(go.Scatter(
            x=df_acc["date"], y=df_acc["actual"],
            name="Actual", mode="lines",
            line=dict(color="#8ab4f8", width=2)
        ))
        # predicted
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
        mape = df_acc["mape_pct"].mean()
        ci_hits = (df_acc["ci_hit"] == "Within CI").sum()
        ci_total = len(df_acc)
        ci_pct = ci_hits / ci_total * 100 if ci_total else 0
        
        st.subheader("Model accuracy summary")
        st.metric("MAPE (Mean Abs % Error)", f"{mape:.1f}%",help="Lower is better. <10% is generally good for daily revenue forecasting.")
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

# Category projection + Top products
st.markdown(
    f'<div class="section-badge">Category & Product Projection</div>',
    unsafe_allow_html=True,
)
col_cat, col_prod = st.columns([1, 1])
# Category revenue projection
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
# Top product projection
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


# Daily forecast breakdown table
st.markdown('<div class="section-badge">Daily Forecast Detail </div>',
            unsafe_allow_html=True)
if not df_fc.empty:
    col_tbl, col_bar = st.columns([2, 3])
    # Daily forecast table
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
    # Daily forecast bar chart with error bars
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