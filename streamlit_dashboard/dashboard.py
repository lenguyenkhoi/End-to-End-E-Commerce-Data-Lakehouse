import streamlit as st
from shared.config import CSS

st.set_page_config(page_title="KHOI E-Commerce ", layout="wide")
st.markdown(CSS, unsafe_allow_html=True)

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

# page main 
st.title("Welcome to KHOI E-Commerce Dashboard")
st.write("""
    This dashboard provides a comprehensive view of your e-commerce business, combining real-time streaming data,
    historical sales performance, forecasting, and AI-driven insights. Use the navigation links in the sidebar to explore different sections of the dashboard:
- **Real-time Stream**: Monitor live events and key metrics as they happen.
- **Sales Dashboard**: Analyze historical sales data, trends, and performance.
- **Forecast Dashboard**: Get data-driven forecasts for future sales and demand.
- **AI Advisor**: Receive AI-generated insights and recommendations based on your data.
    """)