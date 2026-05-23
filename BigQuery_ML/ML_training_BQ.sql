CREATE OR REPLACE MODEL `ecommerce-streaming-pipeline.gold_ecommerce.revenue_forecast_model`
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'time_bucket', -- Dùng cột đã truncate
  time_series_data_col = 'net_revenue',
  holiday_region = 'VN',
  auto_arima = TRUE
) AS
SELECT
  -- Ép tất cả giao dịch trong cùng 1 phút về cùng 1 mốc thời gian
  TIMESTAMP_TRUNC(order_ts, MINUTE) AS time_bucket,
  SUM(net_revenue) AS net_revenue
FROM `ecommerce-streaming-pipeline.gold_ecommerce.fact_sales_stream`
GROUP BY 1