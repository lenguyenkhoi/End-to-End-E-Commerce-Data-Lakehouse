import streamlit as st
import uuid
from psycopg2.extras import RealDictCursor
from shared.db import init_connection
from shared.kafka_client import send_kafka_event, TOPIC_INVENTORY, TOPIC_ORDER, TOPIC_REVIEW
from shared.state import apply_discount, navigate
from shared.cart_actions import PRODUCTS


def checkout_order(subtotal, payment_method = "COD"):
    """_summary_ Xử lý thanh toán đơn hàng, lưu thông tin đơn hàng vào database, cập nhật tồn kho, gửi sự kiện đến kafka qua topic_order và topic_inventory
    """
    conn = init_connection()
    order_id = "ORD_" + uuid.uuid4().hex[:8].upper()
    user_id = st.session_state.user["user_id"]
    promo = st.session_state.applied_promo

    shipping_fee = 15000 if subtotal < 10_000_000 else 0
    discount_amount, discounted_subtotal = apply_discount(subtotal, promo)
    total_amount = discounted_subtotal + shipping_fee
    promo_id = promo["promo_id"] if promo else None

    is_success = False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO orders
                    (order_id, user_id, promo_id, subtotal, shipping_fee,
                     discount_amount, total_amount, payment_method, order_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'Pending')
            """, (order_id, user_id, promo_id, subtotal, shipping_fee,
                  discount_amount, total_amount, payment_method))

            kafka_items = []
            for pid, qty in st.session_state.cart.items():
                p = next((x for x in PRODUCTS if x["id"] == pid), None)
                if not p:
                    continue
                item_total = qty * p["price"]
                cur.execute("""
                    INSERT INTO order_items
                        (order_id, product_id, quantity, unit_price, unit_cost, item_total)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (order_id, pid, qty, p["price"], p["cost"], item_total))

                cur.execute(
                    "UPDATE inventory SET stock_quantity = stock_quantity - %s WHERE product_id = %s",
                    (qty, pid)
                )
                cur.execute("""
                    INSERT INTO inventory_logs
                        (product_id, order_id, transaction_type, quantity_changed, note)
                    VALUES (%s, %s, 'OUT', %s, 'Real User Checkout')
                """, (pid, order_id, qty))

                # Low-stock alert event
                cur.execute("SELECT stock_quantity FROM inventory WHERE product_id = %s", (pid,))
                new_stock = cur.fetchone()[0]
                if new_stock < 10:
                    send_kafka_event(TOPIC_INVENTORY, "low_stock_alert", {
                        "product_id": pid, "product_name": p["name"],
                        "stock_remaining": new_stock,
                    })

                kafka_items.append({"product_id": pid, "product_name": p["name"], "quantity": qty, "unit_price": p["price"], "unit_cost": p["cost"]})

        conn.commit()
        is_success = True

        send_kafka_event(TOPIC_ORDER, "order_created", {
            "order_id": order_id,
            "subtotal": subtotal,
            "shipping_fee": shipping_fee,
            "discount_amount": discount_amount,
            "total_amount": total_amount,
            "payment_method": payment_method,
            "promo_id": promo_id,
            "promo_code": promo["promo_code"] if promo else None,
            "order_status": "Pending",
            "items": kafka_items,
        })

    except Exception as e:
        conn.rollback()
        st.error(f"Lỗi khi thanh toán: {e}")

    if is_success:
        st.session_state.cart = {}
        st.session_state.applied_promo = None
        st.session_state.notification = "🎉 Đặt hàng thành công! Đơn hàng đang được xử lý."
        navigate("profile")

# ══════════════════════════════════════════════════════════════════
# INVENTORY MANAGEMENT
# ══════════════════════════════════════════════════════════════════

def restock_item(product_id, quantity):
    """_summary_ Nhập Kho, cập nhật số lượng tồn kho và lưu vào databse

    Args:
        product_id (_type_): _description_
        quantity (_type_): _description_

    Returns:
        _type_: _description_
    """
    conn = init_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT product_name FROM products WHERE product_id = %s", (product_id,))
            p_res = cur.fetchone()
            p_name = p_res[0] if p_res else ""

            cur.execute(
                "UPDATE inventory SET stock_quantity = stock_quantity + %s, last_restock_date = CURRENT_TIMESTAMP WHERE product_id = %s RETURNING stock_quantity",
                (quantity, product_id)
            )
            new_stock = cur.fetchone()[0]

            cur.execute(
                "INSERT INTO inventory_logs (product_id, transaction_type, quantity_changed, note) VALUES (%s, 'IN', %s, 'Manual Restock')",
                (product_id, quantity)
            )
        conn.commit()
        send_kafka_event(TOPIC_INVENTORY, "restock_manual", {
            "product_id": product_id, 
            "product_name": p_name,          
            "quantity_added": quantity,
            "stock_remaining": new_stock,     
        })
        return True
    except:
        conn.rollback()
        return False
    

def auto_restock(threshold=15, restock_amount=50):
    """_summary_ Kiểm tra các sản phẩm trong kho nếu số lượng dưới ngưỡng sẽ tự động nhập thêm

    Args:
        threshold (int, optional): _description_. Defaults to 15. Ngưỡng để xác định sản phẩm nào cần được nhập thêm.
        restock_amount (int, optional): _description_. Defaults to 50. Số lượng sẽ được thêm vào cho mỗi sản phẩm cần nhập thêm.

    Returns:
        _type_: _description_ Số lượng sản phẩm đã được tự động nhập thêm, trả về -1 nếu có lỗi xảy ra trong quá trình thực hiện
    """
    conn = init_connection()
    if not conn:
        return -1
    restocked_count = 0
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT i.product_id, p.product_name FROM inventory i JOIN products p ON i.product_id = p.product_id WHERE i.stock_quantity <= %s FOR UPDATE",
                (threshold,)
            )
            low_stock_items = cur.fetchall()
            for item in low_stock_items:
                pid = item["product_id"]
                p_name = item["product_name"]
                cur.execute(
                    "UPDATE inventory SET stock_quantity = stock_quantity + %s, last_restock_date = CURRENT_TIMESTAMP WHERE product_id = %s RETURNING stock_quantity",
                    (restock_amount, pid)
                )
                new_stock = cur.fetchone()["stock_quantity"]
                cur.execute(
                    "INSERT INTO inventory_logs (product_id, transaction_type, quantity_changed, note) VALUES (%s, 'IN', %s, 'Restock')",
                    (pid, restock_amount)
                )
                send_kafka_event(TOPIC_INVENTORY, "restock_auto", {
                    "product_id": pid,
                    "product_name": p_name,             
                    "quantity_added": restock_amount,
                    "stock_remaining": new_stock,      
                })
                restocked_count += 1
        conn.commit()
        return restocked_count
    except:
        conn.rollback()
        return -1

# ══════════════════════════════════════════════════════════════════
# REVIEW
# ══════════════════════════════════════════════════════════════════
def submit_review(product_id, rating, comment):
    conn = init_connection()
    user_id = st.session_state.user["user_id"]
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO reviews (product_id, user_id, rating, comment)
                VALUES (%s, %s, %s, %s)
            """, (product_id, user_id, rating, comment))
        conn.commit()
        send_kafka_event(TOPIC_REVIEW, "review_submitted", {
            "product_id": product_id, "rating": rating,
            "comment_length": len(comment),
        })
        return True
    except:
        conn.rollback()
        return False