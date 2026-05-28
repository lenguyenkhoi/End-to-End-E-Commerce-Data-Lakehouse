import os

# BigQuery config
PROJECT_ID       = "ecommerce-streaming-pipeline"          
DATASET_GOLD     = "gold_ecommerce"
SERVICE_ACCT_KEY = "3_spark_processing/config/gcp_service_account.json"  
G = f"{PROJECT_ID}.{DATASET_GOLD}"  # shorthand for SQL queries

# Gemini AI config
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")  
GEMINI_MODEL   = "gemini-2.5-flash"             
# dashboard config
REFRESH_INTERVAL = 30          # seconds between live data polls
STREAM_LOOKBACK  = 24          # hours of streaming data to display

# Custom CSS for Streamlit Dashboard
CSS = """
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

  /* Đã lược bỏ phần 2: .stPlotlyChart để biểu đồ trở về trạng thái gốc */

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
"""