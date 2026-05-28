import streamlit as st
import google.generativeai as genai
from shared.bq import run_cached_query, run_query
from shared.queries import *
from shared.config import GEMINI_API_KEY, GEMINI_MODEL

# sidebar controls
st.markdown("""
    <style>
        [data-testid="stSidebarNav"] {
            display: none;
        }
    </style>
""", unsafe_allow_html=True) #ẩn các pagelink

with st.sidebar:
    st.header("Navigation")
    st.page_link("dashboard.py", label = "🏠 Home")
    st.page_link("pages/event_stream.py",label =  "📈 Real-time Stream")
    st.page_link("pages/sales_dashboard.py", label =  "📊 Sales Dashboard")
    st.page_link("pages/forecasting.py", label =  "🔮 Forecast Dashboard")
    st.page_link("pages/ai_advisor.py" , label = "🤖 AI Advisor")

# Context builder: pull actual data from BigQuery 
def build_ai_context(days= 30):
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


# Helper functions for prompt construction and Gemini API call
def df_to_text(df, title, max_rows = 10):
    """Converts a DataFrame to a compact markdown-style text block for the prompt."""
    if df is None or df.empty:
        return f"\n### {title}\nNo data available.\n"
    rows = df.head(max_rows).to_string(index=False)
    return f"\n### {title}\n{rows}\n"

# System prompt builder and Gemini API call with streaming response
def build_system_prompt(ctx, analysis_mode, days) :
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

# call Gemini
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

# Sidebar controls for this page 
with st.sidebar:
    st.subheader("AI settings")
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

# PAGE LAYOUT 
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
# Load data context 
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

# Show live data snapshot so user can see what AI is reading
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

# Chat session state init
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

# Quick-start buttons (auto-trigger analysis) 
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

# Display chat history
for msg in st.session_state.ai_messages:
    with st.chat_message(msg["role"],
                            avatar="🤖" if msg["role"] == "assistant" else "👤"):
        st.markdown(msg["content"])

# Handle auto-prompt from quick-start buttons 
auto = st.session_state.pop("_auto_prompt", None)

# Chat input 
user_input = st.chat_input(
    placeholder="Ask about your data — e.g. 'Why is margin dropping?' or 'Which promo should I run?'",
    key="ai_chat_input",
)
if auto and not user_input:
    user_input = auto

# Process user message
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

# Clear chat button
if st.session_state.ai_messages:
    if st.button("🗑️ Clear conversation", key="ai_clear"):
        st.session_state.ai_messages    = []
        st.session_state.gemini_history = []
        st.rerun()