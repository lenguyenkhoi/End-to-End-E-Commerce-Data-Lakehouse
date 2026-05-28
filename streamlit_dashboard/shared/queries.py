from shared.config import G

# Streaming Queries
def sql_stream_kpis(hours):
    """
    Generates a SQL query to fetch key performance indicators (KPIs) for an e-commerce dashboard.
    the logic: 
    - Orders: count new orders, cancellations,returns, and paid orders from the fact_orders_stream
    - Sales: sum net_revenue from fact_sales_stream
    - Users: count distinct active users from fact_user_events with event_type "user_login"
    - Pages: count page views from fact_page_views
    """
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

#_orders_per_minute 
def sql_orders_per_minute(hours):
    """
    Rolling order count grouped by 1-minute buckets.
    Uses fact_orders_stream with is_new_order and is_cancelled flags.
    """
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

# page_view_breakdown
def sql_page_view_breakdown(hours):
    """
    Breakdown of page views by event type.
    Uses fact_page_views to count different types of page view events.
    """
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

# device_logins
def sql_device_logins(hours):
    """
    logins by device type for user login in the last N hours
    """
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

# top_viewed_prodcuts
def sql_top_viewed_products(hours,limit = 10):
    """
    Top viewed products in the last N hours.
    uses fact_page_views filtered by event_type = "product_viewed" and non-null product_name.
    """
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

# live_revenue_category
def sql_live_revenue_category(hours):
    """
    Revenur by category in the last N hours.
    join fact_sales_stream with dim_products to get category
    sum net_revenue
    """
    return f"""
    SELECT p.category, SUM(f.net_revenue) AS revenue
    FROM {G}.fact_sales_stream f
    JOIN {G}.dim_products p 
    ON f.product_id = p.product_id
    WHERE f.order_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    GROUP BY 1 
    ORDER BY 2 DESC
    """

# trending_searches
def sql_trending_searches(hours):
    """
    Trending search items -> marketing
    """
    return f"""
    SELECT search_item, COUNT(*) AS search_count
    FROM {G}.fact_page_views
    WHERE event_type = 'search_performed' 
      AND search_item IS NOT NULL
      AND event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    GROUP BY 1 ORDER BY 2 DESC LIMIT 8
    """

# live_inventory_alerts
def sql_live_inventory_alerts(hours):
    """
    Live inventory alerts for the last N hours.
    """
    return f"""
    SELECT product_name, stock_remaining, FORMAT_TIMESTAMP('%H:%M:%S', event_ts) AS time
    FROM {G}.fact_inventory_events
    WHERE is_alert = TRUE
      AND event_ts >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
    ORDER BY event_ts DESC LIMIT 5
    """
# stock_direction
def sql_stock_direction(hours):
    """
    Stock direction (in and out)
    """
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

# Sales dashboard quries in gold batch tables

# time filter helper
def get_time_filter(date_col,days,years,quarters):
    """
    Auto-generates a SQL WHERE clause for filtering by date based on user-selected parameters.
    - date_col: the name of the date column to filter on (e.g. "date_key")
    - days: number of recent days to include (used if years and quarters are empty)
    - years: list of specific years to include (e.g. [2023, 2024])
    - quarters: list of specific quarters to include (e.g. [1, 2])
    """
    conditions = []
    if years:
        conditions.append(f"EXTRACT(YEAR FROM {date_col}) IN ({','.join(map(str, years))})")
    if quarters:
        conditions.append(f"EXTRACT(QUARTER FROM {date_col}) IN ({','.join(map(str, quarters))})")
    
    if conditions:
        return " AND ".join(conditions)
    else:
        return f"{date_col} >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)"

# KPIS
def sql_batch_kpis(time_filter):
    """
    KPIS including:
    - total_orders: count distinct order_id from fact_orders
    - total_revenue: sum net_revenue from fact_sales
    - avg_order_value: average total_amount from fact_orders
    - avg_margin_pct: average gross_margin_pct from fact_sales
    - unique_customers: count distinct user_id from fact_sales
    joins fact_orders with fact_sales and dim_order_status to exclude negative orders (cancellations/returns)
    """
    return f"""
    SELECT
        COUNT(DISTINCT fo.order_id) AS total_orders,
        SUM(fs.net_revenue) AS total_revenue,
        AVG(fo.total_amount) AS avg_order_value,
        AVG(fs.gross_margin_pct) AS avg_margin_pct,
        COUNT(DISTINCT fs.user_id) AS unique_customers
    FROM {G}.fact_sales fs
    JOIN {G}.fact_orders fo ON fo.order_id = fs.order_id
    JOIN {G}.dim_order_status ds ON ds.status_code = fo.order_status
    WHERE {time_filter} AND NOT ds.is_negative
    """

# monthly_revenue
def sql_monthly_revenue(time_filter):
    return f"""
    SELECT
        DATE_TRUNC(fs.date_key, MONTH) AS month,
        SUM(fs.net_revenue) AS net_revenue,
        COUNT(DISTINCT fo.order_id) AS order_count,
        AVG(fs.gross_margin_pct) AS avg_margin
    FROM {G}.fact_sales fs
    JOIN {G}.fact_orders fo ON fo.order_id = fs.order_id
    WHERE {time_filter}
    GROUP BY 1 ORDER BY 1
    """

# category revenue
def sql_category_revenue(time_filter):
    return f"""
    SELECT
        category, SUM(net_revenue) AS net_revenue, SUM(gross_profit) AS gross_profit,
        SUM(quantity) AS units_sold, AVG(gross_margin_pct) AS avg_margin
    FROM {G}.fact_sales
    WHERE {time_filter}
    GROUP BY 1 ORDER BY 2 DESC
    """
# payment split
def sql_payment_split(time_filter):
    return f"""
    SELECT
        dp.payment_method_name, dp.payment_channel, dp.is_digital,
        COUNT(fo.order_id) AS order_count, SUM(fo.total_amount) AS total_amount
    FROM {G}.fact_orders          fo
    JOIN {G}.dim_payment_method   dp ON dp.payment_method_code = fo.payment_method
    WHERE {time_filter}
    GROUP BY 1, 2, 3 ORDER BY 4 DESC
    """

# order_status_funnel
def sql_order_status_funnel(time_filter):
    return f"""
    SELECT
        DATE_TRUNC(fo.date_key, MONTH) AS month, 
        ds.status_name_vi AS status,
        ds.is_negative, 
        COUNT(fo.order_id) AS orders
    FROM {G}.fact_orders fo
    JOIN {G}.dim_order_status ds 
    ON ds.status_code = fo.order_status
    WHERE {time_filter}
    GROUP BY 1, 2, 3 
    ORDER BY 1, 2
    """
# top_products
def sql_top_products(time_filter, limit = 10):
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
# promo_performance
def sql_promo_performance(time_filter):
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
# profit by location
def sql_profit_by_location(time_filter):
    return f"""
    SELECT
        du.location,
        SUM(fs.gross_profit) AS gross_profit
    FROM {G}.fact_sales fs
    JOIN {G}.dim_users du ON fs.user_id = du.user_id
    WHERE {time_filter}
    GROUP BY 1 ORDER BY 2 ASC
   """
   
# Forecasting queries

# forecast
def sql_forecast(horizon, conf):
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
            MODEL `{G}.revenue_forecast_model`,
            STRUCT({horizon} AS horizon, {conf} AS confidence_level)
        )
        ORDER BY date
        """
# history
def sql_history(days):
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
    FROM `{G}.fact_sales`
    WHERE date_key >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    GROUP BY 1
    ORDER BY 1
    """

# forecast vs actual
def sql_forecast_vs_actual(horizon: int, conf: float) -> str:
    """
    Compares forecast vs actuals for the last 30 days.
    Joins the ML.FORECAST output with actuals from fact_sales to calculate MAPE
    """
    return f"""
    WITH fc AS (
        SELECT
            DATE_SUB(DATE(forecast_timestamp), INTERVAL 30 DAY) AS date,
            forecast_value                        AS predicted,
            prediction_interval_lower_bound       AS lower_bound,
            prediction_interval_upper_bound       AS upper_bound
        FROM ML.FORECAST(
            MODEL `{G}.revenue_forecast_model`,
            STRUCT({horizon} AS horizon, {conf} AS confidence_level)
        )
    ),
    actuals AS (
        SELECT date_key AS date, SUM(net_revenue) AS actual
        FROM `{G}.fact_sales`
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

# category forecast
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
        FROM `{G}.fact_sales`
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

# top products forecast
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
        FROM `{G}.fact_sales`
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