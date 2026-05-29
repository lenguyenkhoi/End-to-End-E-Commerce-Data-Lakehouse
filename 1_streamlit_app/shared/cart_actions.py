import streamlit as st
from psycopg2.extras import RealDictCursor
from shared.db import init_connection, get_all_products
from shared.kafka_client import send_kafka_event, TOPIC_CART, TOPIC_PROMO


PRODUCTS = get_all_products()
CATEGORIES = ["All"] + sorted(set(p["category"] for p in PRODUCTS))

def add_to_cart(product_id):
    prev_qty = st.session_state.cart.get(product_id, 0)
    st.session_state.cart[product_id] = prev_qty + 1
    p = next((x for x in PRODUCTS if x["id"] == product_id), None)
    if p:
        st.session_state.notification = f"✓ {p['name']} đã được thêm vào giỏ hàng!"
        send_kafka_event(TOPIC_CART, "item_added", {
            "product_id": product_id, "product_name": p["name"],
            "brand": p["brand"], "category": p["category"],
            "unit_price": p["price"], "quantity": 1,
            "cart_quantity_after": prev_qty + 1,
            "old_quantity": prev_qty,
            "new_quantity": prev_qty + 1,
        })


def remove_from_cart(product_id):
    if product_id in st.session_state.cart:
        old_qty = st.session_state.cart[product_id]
        del st.session_state.cart[product_id]
        p = next((x for x in PRODUCTS if x["id"] == product_id), None)
        if p:
            send_kafka_event(TOPIC_CART, "item_removed", {
                "product_id": product_id,
                "product_name": p["name"],
                "brand": p["brand"],
                "category": p["category"],
                "unit_price": p["price"],
                "old_quantity": old_qty,
                "new_quantity": 0,
                "quantity": -old_qty,
                "cart_quantity_after": 0,
            })




def update_cart_qty(product_id, new_qty):
    old_qty = st.session_state.cart.get(product_id, 0)
    st.session_state.cart[product_id] = new_qty
    p = next((x for x in PRODUCTS if x["id"] == product_id), None)
    if p:
        send_kafka_event(TOPIC_CART, "item_qty_updated", {
            "product_id": product_id,
            "product_name": p["name"],
            "brand": p["brand"],
            "category": p["category"],
            "unit_price": p["price"],
            "old_quantity": old_qty, 
            "new_quantity": new_qty,
            "quantity": new_qty - old_qty,
            "cart_quantity_after": new_qty,
        })


def apply_promo_code(code: str):
    """Kiểm tra mã giảm giá và lưu vào session nếu hợp lệ."""
    try:
        conn = init_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT promo_id, promo_code, discount_percent
                FROM promotions
                WHERE promo_code = %s AND is_active = TRUE
                  AND (start_date IS NULL OR start_date <= NOW())
                  AND (end_date IS NULL OR end_date >= NOW())
            """, (code.strip().upper(),))
            promo = cur.fetchone()
        if promo:
            st.session_state.applied_promo = dict(promo)
            send_kafka_event(TOPIC_PROMO, "promo_applied", {
                "promo_code": promo["promo_code"],
                "promo_id": promo["promo_id"],
                "discount_percent": float(promo["discount_percent"]),
            })
            return True, f"Áp dụng mã **{promo['promo_code']}** — giảm {promo['discount_percent']}%!"
        send_kafka_event(TOPIC_PROMO, "promo_invalid", {"promo_code": code})
        return False, "Mã giảm giá không hợp lệ hoặc đã hết hạn."
    except Exception as e:
        return False, f"Lỗi kiểm tra mã: {e}"

def remove_promo():
    if st.session_state.applied_promo:
        send_kafka_event(TOPIC_PROMO, "promo_removed", {
            "promo_code": st.session_state.applied_promo.get("promo_code", ""),
        })
    st.session_state.applied_promo = None