import os
import streamlit as st 
from psycopg2.extras import RealDictCursor
import psycopg2


@st.cache_resource
def init_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "ecom_user"),
        password=os.getenv("DB_PASS", "ecom_password")
    )


def get_all_products():
    """Lấy danh sách sản phẩm đang hoạt động, kết hợp thông tin tồn kho."""
    try:
        conn = init_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT p.product_id AS id, p.product_name AS name, p.brand,
                       p.category, p.price, p.image_url,
                       i.stock_quantity AS stock, i.import_price AS cost
                FROM products p
                LEFT JOIN inventory i ON p.product_id = i.product_id
                WHERE p.is_active = TRUE
                ORDER BY p.price DESC;
            """)
            rows = cur.fetchall()
            products = []
            for r in rows:
                p = dict(r)
                p['tag'] = (
                    "Sắp hết hàng" if p['stock'] and p['stock'] < 15
                    else ("Bán chạy" if p['price'] > 20000000 else None)
                )
                products.append(p)
            return products
    except Exception as e:
        st.error(f"Lỗi DB: {e}")
        return []


def get_active_promotions():
    """Lấy danh sách mã giảm giá đang hoạt động từ bảng promotions."""
    try:
        conn = init_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT promo_id, promo_code, discount_percent
                FROM promotions
                WHERE is_active = TRUE
                  AND (start_date IS NULL OR start_date <= NOW())
                  AND (end_date IS NULL OR end_date >= NOW())
                ORDER BY discount_percent DESC;
            """)
            return [dict(r) for r in cur.fetchall()]
    except Exception as e:
        st.error(f"Lỗi DB: {e}")
        return []
