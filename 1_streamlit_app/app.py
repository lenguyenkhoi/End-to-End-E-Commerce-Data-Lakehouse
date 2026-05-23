import streamlit as st
import time
import os
import uuid
import json
import psycopg2
import base64
import random
import unicodedata
import string
from datetime import datetime, timedelta
from psycopg2.extras import RealDictCursor
from kafka import KafkaProducer

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="KHOI Store",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&family=DM+Sans:wght@300;400;500&display=swap');
:root { --cream: #F7F3EE; --dark: #1A1714; --accent: #C8956C; --muted: #8A7F78; --card-bg: #FFFFFF; --border: #E8E0D8; }
html, body, [data-testid="stAppViewContainer"] { background-color: var(--cream) !important; font-family: 'DM Sans', sans-serif; color: var(--dark); }
[data-testid="stHeader"] { background: transparent !important; }
[data-testid="stSidebar"] { background: var(--dark) !important; }
[data-testid="stSidebar"] * { color: var(--cream) !important; }
h1, h2, h3 { font-family: 'Playfair Display', serif !important; }
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1rem !important; }
.navbar { display: flex; justify-content: space-between; align-items: center; padding: 1rem 2rem; background: var(--dark); border-radius: 16px; margin-bottom: 2rem; color: var(--cream); }
.navbar-brand { font-family: 'Playfair Display', serif; font-size: 1.8rem; font-weight: 700; color: var(--accent) !important; letter-spacing: 2px; }
.navbar-links { display: flex; gap: 2rem; align-items: center; }
.nav-link { color: var(--cream) !important; font-size: 0.9rem; font-weight: 500; text-transform: uppercase; opacity: 0.8;}
.cart-badge { background: var(--accent); color: white; border-radius: 50%; padding: 2px 8px; font-size: 0.75rem; font-weight: 700; margin-left: 4px; }
.hero { background: linear-gradient(135deg, var(--dark) 0%, #3D2E25 100%); border-radius: 20px; padding: 4rem 3rem; margin-bottom: 3rem; text-align: center; }
.hero-eyebrow { color: var(--accent); font-size: 0.8rem; letter-spacing: 4px; text-transform: uppercase; margin-bottom: 1rem; }
.hero-title { font-family: 'Playfair Display', serif; font-size: 3.5rem; font-weight: 700; color: var(--cream); line-height: 1.2; margin-bottom: 1.2rem; }
.hero-sub { color: rgba(247,243,238,0.7); font-size: 1.1rem; max-width: 500px; margin: 0 auto 2rem; }
.section-heading { font-family: 'Playfair Display', serif; font-size: 2rem; font-weight: 600; margin-bottom: 0.25rem; }
.section-sub { color: var(--muted); font-size: 0.9rem; margin-bottom: 2rem; text-transform: uppercase; }
.divider { width: 50px; height: 3px; background: var(--accent); margin-bottom: 2rem; }
.product-card { background: var(--card-bg); border-radius: 16px; border: 1px solid var(--border); transition: transform 0.25s, box-shadow 0.25s; height: 100%; padding-bottom: 1rem;}
.product-card:hover { transform: translateY(-4px); box-shadow: 0 16px 40px rgba(26,23,20,0.12); }
.product-img { width: 100%; height: 200px; display: flex; align-items: center; justify-content: center; font-size: 4rem; background: var(--cream); }
.product-body { padding: 1.25rem; }
.product-name { font-family: 'Playfair Display', serif; font-size: 1.1rem; font-weight: 600; margin-bottom: 0.4rem; }
.product-price { font-size: 1.2rem; font-weight: 700; color: var(--dark); }
.stButton > button { background: var(--dark) !important; color: var(--cream) !important; border-radius: 10px !important; font-weight: 500 !important; padding: 0.55rem 1.4rem !important; transition: background 0.2s !important; }
.stButton > button:hover { background: var(--accent) !important; }
.toast { background: var(--dark); color: var(--cream); border-left: 4px solid var(--accent); padding: 0.9rem 1.4rem; border-radius: 10px; margin-bottom: 1rem; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════
# DATABASE & KAFKA CONNECTIONS
# ══════════════════════════════════════════════════════════════════
@st.cache_resource
def init_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "ecommerce_db"),
        user=os.getenv("DB_USER", "ecom_user"),
        password=os.getenv("DB_PASS", "ecom_password")
    )

@st.cache_resource
def get_kafka_producer():
    try:
        return KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_SERVER", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
    except:
        return None

# ══════════════════════════════════════════════════════════════════
# DATA FETCHING
# ══════════════════════════════════════════════════════════════════
def get_all_products():
    """Lấy danh sách sản phẩm đang hoạt động, kết hợp thông tin tồn kho để hiển thị tag "Sắp hết hàng" hoặc "Bán chạy"."""   
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
    except:
        return []

PRODUCTS = get_all_products()
CATEGORIES = ["All"] + sorted(set(p["category"] for p in PRODUCTS))

# ══════════════════════════════════════════════════════════════════
# SESSION STATE
# ══════════════════════════════════════════════════════════════════
def init_state():
    defaults = {
        "logged_in": False,
        "user": None,
        "cart": {},
        "page": "home",
        "notification": None,
        "session_id": str(uuid.uuid4()),
        "applied_promo": None,   # {"promo_id": ..., "promo_code": ..., "discount_percent": ...}
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_state()

# ══════════════════════════════════════════════════════════════════
# KAFKA HELPERS
# ══════════════════════════════════════════════════════════════════

# ── Kafka topics used in this app ─────────────────────────────────
# ecommerce.user          → user_registered, user_login, user_logout, user_login_failed
# ecommerce.page_view     → page_viewed, product_viewed, category_filtered, search_performed
# ecommerce.cart          → item_added, item_removed, item_qty_updated, cart_cleared
# ecommerce.promo         → promo_applied, promo_removed, promo_invalid
# ecommerce.order         → order_created, order_paid, order_cancelled, order_returned
# ecommerce.inventory     → stock_updated_out, stock_updated_in, low_stock_alert, restock_manual, restock_auto
# ecommerce.review        → review_submitted

TOPIC_USER      = "ecommerce.user"
TOPIC_PAGE_VIEW = "ecommerce.page_view"
TOPIC_CART      = "ecommerce.cart"
TOPIC_PROMO     = "ecommerce.promo"
TOPIC_ORDER     = "ecommerce.order"
TOPIC_INVENTORY = "ecommerce.inventory"
TOPIC_REVIEW    = "ecommerce.review"

def _build_event(event_type: str, payload: dict, override_ts: str = None,
                 override_user: dict = None, override_session: str = None) -> dict:
    """Tạo envelope chuẩn cho mọi Kafka event.
    
    FIX: Always include location, device_type, os in every event envelope.
    Previously these were only added by send_sim (simulation path), meaning
    live send_kafka_event calls produced NULL for those columns in BigQuery.
    Now _build_event injects them from session state / user dict so the
    streaming schema always gets non-null values.
    """
    user = override_user or (st.session_state.user if st.session_state.logged_in else None)
    # Pop context fields from payload if caller already set them (e.g. send_sim),
    # otherwise fall back to session-state / user data.
    location    = payload.pop("location",    user.get("location", "unknown") if user else "unknown")
    device_type = payload.pop("device_type", "unknown")
    os_name     = payload.pop("os",          "unknown")
    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  event_type,
        "timestamp":   override_ts or datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "session_id":  override_session or st.session_state.get("session_id", ""),
        "user_id":     user["user_id"] if user else "anonymous",
        "user_name":   user.get("name", "") if user else "",
        "location":    location,
        "device_type": device_type,
        "os":          os_name,
        **payload,
    }

def send_kafka_event(topic: str, event_type: str, payload: dict,
                     override_ts: str = None, override_user: dict = None,
                     override_session: str = None):
    producer = get_kafka_producer()
    if not producer:
        return
    # TỰ ĐỘNG ĐIỀN PAGE_NAME DỰA VÀO TRANG NGƯỜI DÙNG ĐANG ĐỨNG
    if topic == TOPIC_PAGE_VIEW and "page_name" not in payload:
        payload["page_name"] = st.session_state.get("page", "unknown")
    event = _build_event(event_type, payload, override_ts, override_user, override_session)
    try:
        producer.send(topic, value=event)
        producer.flush(timeout=1.0)
    except:
        pass

# ══════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════
def random_past_datetime(days_back: int = 100) -> datetime:
    delta = timedelta(
        days=random.randint(0, days_back),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
    )
    return datetime.now() - delta

def random_past_str(days_back: int = 100) -> str:
    return random_past_datetime(days_back).strftime('%Y-%m-%d %H:%M:%S')

def cart_count():
    return sum(st.session_state.cart.values())

def cart_subtotal():
    return sum(
        next((p["price"] for p in PRODUCTS if p["id"] == pid), 0) * qty
        for pid, qty in st.session_state.cart.items()
    )

def apply_discount(subtotal: int, promo) -> tuple[int, int]:
    """Returns (discount_amount, total_after_discount)."""
    if not promo:
        return 0, subtotal
    discount_amount = int(subtotal * promo["discount_percent"] / 100)
    return discount_amount, subtotal - discount_amount

def navigate(page):
    st.session_state.page = page
    send_kafka_event(TOPIC_PAGE_VIEW, "page_viewed", {"page_name": page})
    st.rerun()

# ══════════════════════════════════════════════════════════════════
# AUTH
# ══════════════════════════════════════════════════════════════════
def login(username, password):
    try:
        conn = init_connection()
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "SELECT user_id, username, full_name AS name, location, gender, email, phone "
                "FROM users WHERE username = %s AND password = %s",
                (username, password)
            )
            user = cur.fetchone()
            if user:
                st.session_state.logged_in = True
                st.session_state.user = dict(user)
                st.session_state.notification = f"Mừng trở lại, {user['name']}! 👋"
                send_kafka_event(TOPIC_USER, "user_login", {
                    "username": username,
                    "location": user.get("location", ""),
                })
                return True
        send_kafka_event(TOPIC_USER, "user_login_failed", {"username": username})
    except:
        pass
    return False

def register(name, email, phone, gender, location, username, password):
    conn = init_connection()
    user_id = "U" + uuid.uuid4().hex[:6].upper()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO users (user_id, username, password, full_name, email, phone, gender, location)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, username, password, name, email, phone, gender, location))
        conn.commit()
        send_kafka_event(TOPIC_USER, "user_registered", {
            "username": username, "gender": gender, "location": location,
        })
        return True
    except:
        conn.rollback()
        return False

def logout():
    user = st.session_state.get("user", {})
    send_kafka_event(TOPIC_USER, "user_logout", {
        "username": user.get("username", ""),
    })
    st.session_state.logged_in = False
    st.session_state.user = None
    st.session_state.cart = {}
    st.session_state.applied_promo = None
    st.session_state.page = "home"
    st.rerun()

# ══════════════════════════════════════════════════════════════════
# CART ACTIONS
# ══════════════════════════════════════════════════════════════════
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

# def remove_from_cart(product_id):
#     if product_id in st.session_state.cart:
#         del st.session_state.cart[product_id]
#         send_kafka_event(TOPIC_CART, "item_removed", {"product_id": product_id})

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

# def update_cart_qty(product_id, new_qty):
#     old_qty = st.session_state.cart.get(product_id, 0)
#     st.session_state.cart[product_id] = new_qty
#     p = next((x for x in PRODUCTS if x["id"] == product_id), None)
#     send_kafka_event(TOPIC_CART, "item_qty_updated", {
#         "product_id": product_id, 
#         "old_quantity": old_qty, 
#         "new_quantity": new_qty,
#         "quantity": new_qty - old_qty,
#         "cart_quantity_after": new_qty,
#     })

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
        
# ══════════════════════════════════════════════════════════════════
# PROMO
# ══════════════════════════════════════════════════════════════════
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

# ══════════════════════════════════════════════════════════════════
# CHECKOUT
# ══════════════════════════════════════════════════════════════════
def checkout_order(subtotal: int, payment_method: str = "COD"):
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
# def restock_item(product_id, quantity):
#     conn = init_connection()
#     try:
#         with conn.cursor() as cur:
#             cur.execute(
#                 "UPDATE inventory SET stock_quantity = stock_quantity + %s, last_restock_date = CURRENT_TIMESTAMP WHERE product_id = %s",
#                 (quantity, product_id)
#             )
#             cur.execute(
#                 "INSERT INTO inventory_logs (product_id, transaction_type, quantity_changed, note) VALUES (%s, 'IN', %s, 'Manual Restock')",
#                 (product_id, quantity)
#             )
#         conn.commit()
#         send_kafka_event(TOPIC_INVENTORY, "restock_manual", {
#             "product_id": product_id, "quantity_added": quantity,
#         })
#         return True
#     except:
#         conn.rollback()
#         return False

def restock_item(product_id, quantity):
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
            "product_name": p_name,           # <-- THÊM MỚI
            "quantity_added": quantity,
            "stock_remaining": new_stock,     # <-- THÊM MỚI
        })
        return True
    except:
        conn.rollback()
        return False
    

# def auto_restock(threshold=15, restock_amount=50):
#     conn = init_connection()
#     if not conn:
#         return -1
#     restocked_count = 0
#     try:
#         with conn.cursor(cursor_factory=RealDictCursor) as cur:
#             cur.execute(
#                 "SELECT product_id FROM inventory WHERE stock_quantity <= %s FOR UPDATE",
#                 (threshold,)
#             )
#             low_stock_items = cur.fetchall()
#             for item in low_stock_items:
#                 pid = item["product_id"]
#                 cur.execute(
#                     "UPDATE inventory SET stock_quantity = stock_quantity + %s, last_restock_date = CURRENT_TIMESTAMP WHERE product_id = %s",
#                     (restock_amount, pid)
#                 )
#                 cur.execute(
#                     "INSERT INTO inventory_logs (product_id, transaction_type, quantity_changed, note) VALUES (%s, 'IN', %s, 'Restock')",
#                     (pid, restock_amount)
#                 )
#                 send_kafka_event(TOPIC_INVENTORY, "restock_auto", {
#                     "product_id": pid, "quantity_added": restock_amount,
#                 })
#                 restocked_count += 1
#         conn.commit()
#         return restocked_count
#     except:
#         conn.rollback()
#         return -1

def auto_restock(threshold=15, restock_amount=50):
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

# ══════════════════════════════════════════════════════════════════
# DATA SIMULATOR
# ══════════════════════════════════════════════════════════════════

def remove_accents(input_str):
    """Hàm hỗ trợ loại bỏ dấu tiếng Việt để tạo username"""
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return u"".join([c for c in nfkd_form if not unicodedata.combining(c)])
    
def simulate_traffic(num_users: int):
    """
    Giả lập dữ liệu lịch sử Enterprise với timestamps ngẫu nhiên (100 ngày),
    bao gồm promotions, reviews, returns và đầy đủ Kafka events.
    """
    conn = init_connection()
    producer = get_kafka_producer()
    if not producer or not conn:
        return False

    VN_LAST_NAMES = ['Nguyễn', 'Trần', 'Lê', 'Phạm', 'Hoàng', 'Huỳnh', 'Phan', 'Vũ', 'Võ', 'Đặng', 'Bùi', 'Đỗ', 'Hồ', 'Ngô', 'Dương', 'Lý']
    VN_MIDDLE_NAMES = ['Văn', 'Thị', 'Hữu', 'Ngọc', 'Minh', 'Thanh', 'Gia', 'Bảo', 'Hoài', 'Xuân', 'Thu', 'Đức', 'Tài', 'Thành']
    VN_FIRST_NAMES = ['Anh', 'Tuấn', 'Hùng', 'Dũng', 'Linh', 'Lan', 'Trang', 'Hương', 'Hà', 'Khang', 'Khôi', 'Phúc', 'Tâm', 'An', 'Bình', 'Hân', 'Thảo', 'Nhi']
    VN_PROVINCES = ['Hà Nội', 'TP. Hồ Chí Minh', 'Đà Nẵng', 'Hải Phòng', 'Cần Thơ', 'Đồng Nai', 'Bình Dương', 'Bà Rịa - Vũng Tàu', 'Khánh Hòa', 'Lâm Đồng', 'Thừa Thiên Huế', 'Quảng Ninh', 'Nghệ An', 'Thanh Hóa', 'Bắc Ninh', 'Cà Mau', 'Kiên Giang']
    PAYMENT_METHODS = ['COD', 'Credit Card', 'E-Wallet', 'Bank Transfer']
    DEVICES = ['Mobile', 'Desktop', 'Tablet']
    OS_LIST = ['iOS', 'Android', 'Windows', 'MacOS']
    SEARCH_TERMS = ['iPhone', 'laptop gaming', 'tai nghe chống ồn', 'Samsung', 'MacBook', 'tablet học online', 'Xiaomi', 'Sony']

    
    # Lấy toàn bộ promotions hợp lệ để dùng trong sim
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT promo_id, promo_code, discount_percent FROM promotions WHERE is_active = TRUE")
            all_promos = [dict(r) for r in cur.fetchall()]
    except:
        all_promos = []

    current_products = [p for p in PRODUCTS if p.get('stock', 0) > 0]
    if not current_products:
        return False

    for _ in range(num_users):
        days_back = random.randint(1, 730)
        random_hours = random.randint(0, 23)
        random_minutes = random.randint(0, 59)
        user_reg_dt = datetime.now() - timedelta(days=days_back, hours=random_hours, minutes=random_minutes)
        user_reg_str = user_reg_dt.strftime('%Y-%m-%d %H:%M:%S')

        # ── 1. TẠO KHÁCH ẢO ─────────────────────────────────────
        last_name = random.choice(VN_LAST_NAMES)
        middle_name = random.choice(VN_MIDDLE_NAMES)
        first_name = random.choice(VN_FIRST_NAMES)
        full_name = f"{last_name} {middle_name} {first_name}"
        # Ghép Tên + Họ, loại bỏ dấu tiếng Việt, chuyển thành chữ thường và xóa khoảng trắng
        clean_name = remove_accents(f"{first_name}{last_name}").lower().replace(" ", "")
        # Gắn thêm một số ngẫu nhiên từ 10 đến 9999 để đảm bảo không bị trùng lặp
        username = f"{clean_name}{random.randint(10, 9999)}"
        email = f"{username}@gmail.com"
        password = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        sim_user_id = f"U{uuid.uuid4().hex[:6].upper()}"
        phone       = f"09{random.randint(10000000, 99999999)}"
        gender      = random.choice(['Male', 'Female'])
        location    = random.choice(VN_PROVINCES)
        session_id  = str(uuid.uuid4())
        device      = random.choice(DEVICES)
        os_sys      = random.choice(OS_LIST)

        sim_user_obj = {"user_id": sim_user_id, "name": full_name}

        def send_sim(topic: str, event_type: str, payload: dict, ts: str = user_reg_str):
            """Helper: gửi event với timestamp lịch sử."""
            # 🌟 TỰ ĐỘNG PHÂN TÍCH VÀ ĐIỀN PAGE_NAME CHO SIMULATOR
            if topic == TOPIC_PAGE_VIEW and "page_name" not in payload:
                if event_type == "product_viewed":
                    payload["page_name"] = "product_detail"
                elif event_type in ["search_performed", "category_filtered"]:
                    payload["page_name"] = "products"
                else:
                    payload["page_name"] = "home"
            event = {
                "event_id":   str(uuid.uuid4()),
                "event_type": event_type,
                "timestamp":  ts,
                "session_id": session_id,
                "user_id":    sim_user_id,
                "user_name":  full_name,
                "location":   location,
                "device_type": device,
                "os":          os_sys,
                **payload,
            }
            producer.send(topic, value=event)

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users
                        (user_id, username, password, full_name, email, phone, gender, location, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (sim_user_id, username, "sim123", full_name, email, phone, gender, location, user_reg_str))
            conn.commit()
        except:
            conn.rollback()
            continue

        # ── 2. KAFKA: User Registered ─────────────────────────────
        send_sim(TOPIC_USER, "user_registered", {"username": username, "gender": gender})

        # ── 3. KAFKA: Page Views & Search ─────────────────────────
        # send_sim(TOPIC_PAGE_VIEW, "page_viewed", {"page_name": "home"})

        # if random.random() < 0.6:   # 60% khách tìm kiếm
        #     search_term = random.choice(SEARCH_TERMS)
        #     send_sim(TOPIC_PAGE_VIEW, "search_performed", {
        #         "search_term": search_term,
        #         "results_count": random.randint(3, 15),
        #     })

        # # Browse một vài sản phẩm
        # browsed = random.sample(current_products, min(random.randint(2, 5), len(current_products)))
        # for bp in browsed:
        #     send_sim(TOPIC_PAGE_VIEW, "product_viewed", {
        #         "product_id": bp["id"], "product_name": bp["name"],
        #         "category": bp["category"], "brand": bp["brand"], "price": bp["price"],
        #     })

        # if random.random() < 0.5:
        #     cat = random.choice(CATEGORIES[1:]) if len(CATEGORIES) > 1 else "Smartphone"
        #     send_sim(TOPIC_PAGE_VIEW, "category_filtered", {"category": cat})
        
        # ── 3. KAFKA: Page Views & Search (FULL USER JOURNEY) ─────────
        
        # 3.1. Khách truy cập vào trang chủ
        send_sim(TOPIC_PAGE_VIEW, "page_viewed", {
            "page_name": "home",
            "trigger": "direct_visit"
        }, ts=user_reg_str)

        # 3.2. Khách thực hiện tìm kiếm hoặc lọc danh mục
        if random.random() < 0.6:   # 60% khách tìm kiếm
            search_term = random.choice(SEARCH_TERMS)
            send_sim(TOPIC_PAGE_VIEW, "search_performed", {
                "page_name": "search_results",
                "search_term": search_term,
                "results_count": random.randint(3, 15),
                "trigger": "search_bar"
            }, ts=user_reg_str)
        elif random.random() < 0.5: # Hoặc dùng bộ lọc danh mục
            cat = random.choice(CATEGORIES[1:]) if len(CATEGORIES) > 1 else "Smartphone"
            send_sim(TOPIC_PAGE_VIEW, "category_filtered", {
                "page_name": "category_list",
                "category": cat,
                "trigger": "sidebar_filter"
            }, ts=user_reg_str)

        # 3.3. Khách lướt xem chi tiết nhiều sản phẩm
        browsed = random.sample(current_products, min(random.randint(5, 12), len(current_products)))
        for bp in browsed:
            # Random thêm vài giây/phút để thời gian xem trông thật hơn
            view_ts = (user_reg_dt + timedelta(seconds=random.randint(10, 300))).strftime('%Y-%m-%d %H:%M:%S')
            send_sim(TOPIC_PAGE_VIEW, "product_viewed", {
                "page_name": "product_detail",
                "product_id": bp["id"], 
                "product_name": bp["name"],
                "category": bp["category"], 
                "brand": bp["brand"], 
                "price": bp["price"],
                "trigger": "click_product_card"
            }, ts=view_ts)

        # 3.4. Khách bấm vào xem giỏ hàng trước khi mua
        cart_view_ts = (user_reg_dt + timedelta(minutes=random.randint(5, 10))).strftime('%Y-%m-%d %H:%M:%S')
        send_sim(TOPIC_PAGE_VIEW, "page_viewed", {
            "page_name": "cart",
            "trigger": "click_cart_icon"
        }, ts=cart_view_ts)

        # ── 4. CHỌN SẢN PHẨM ĐỂ MUA ─────────────────────────────
        num_items = random.randint(1, 3)
        chosen_products = random.sample(current_products, min(num_items, len(current_products)))

        # Thời điểm đặt hàng: sau lúc đăng ký vài phút đến vài giờ
        order_offset_minutes = random.randint(5, 180)
        order_dt  = user_reg_dt + timedelta(minutes=order_offset_minutes)
        order_str = order_dt.strftime('%Y-%m-%d %H:%M:%S')

        order_id = f"ORD_{uuid.uuid4().hex[:8].upper()}"
        subtotal = 0
        db_success = False

        # Promo: 30% đơn hàng dùng mã giảm giá
        promo = random.choice(all_promos) if all_promos and random.random() < 0.30 else None
        if promo:
            send_sim(TOPIC_PROMO, "promo_applied", {
                "promo_id": promo["promo_id"],
                "promo_code": promo["promo_code"],
                "discount_percent": float(promo["discount_percent"]),
            }, ts=order_str)

        try:
            with conn.cursor() as cur:
                items_to_buy = []
                for p in chosen_products:
                    cur.execute("SELECT stock_quantity FROM inventory WHERE product_id = %s FOR UPDATE", (p["id"],))
                    res = cur.fetchone()
                    if res and res[0] > 0:
                        actual_qty = min(random.randint(1, 2), res[0])
                        item_total = p["price"] * actual_qty
                        items_to_buy.append({
                            "id": p["id"], "name": p["name"], "price": p["price"],
                            "cost": p["cost"], "qty": actual_qty, "item_total": item_total,
                            "brand": p["brand"], "category": p["category"],
                        })
                        subtotal += item_total

                        # KAFKA: item added to cart
                        send_sim(TOPIC_CART, "item_added", {
                            "product_id": p["id"], "product_name": p["name"],
                            "brand": p["brand"], "category": p["category"],
                            "unit_price": p["price"], "quantity": actual_qty,
                            "cart_quantity_after": actual_qty,                 
                            "old_quantity": 0,                                 
                            "new_quantity": actual_qty,
                        }, ts=order_str)
                        # KAFKA: page_view (add_to_cart trigger)
                        send_sim(TOPIC_PAGE_VIEW, "product_viewed", {
                            "page_name": "product_detail",
                            "product_id": p["id"], "product_name": p["name"],
                            "category": p["category"], "brand": p["brand"], "price": p["price"],
                            "trigger": "add_to_cart",
                        }, ts=order_str)

                if not items_to_buy:
                    conn.rollback()
                    continue

                shipping_fee    = random.choice([0, 15000, 30000])
                discount_amount = int(subtotal * promo["discount_percent"] / 100) if promo else 0
                total_amount    = subtotal - discount_amount + shipping_fee
                payment         = random.choice(PAYMENT_METHODS)

                # Tỷ lệ trạng thái: 70% Delivered, 10% Processing, 10% Pending, 5% Cancelled, 5% Returned
                order_status = random.choices(
                    ['Delivered', 'Processing', 'Pending', 'Cancelled', 'Returned'],
                    weights=[70, 10, 10, 5, 5]
                )[0]

                cur.execute("""
                    INSERT INTO orders
                        (order_id, user_id, promo_id, subtotal, shipping_fee,
                         discount_amount, total_amount, payment_method, order_status,
                         order_date, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (order_id, sim_user_id, promo["promo_id"] if promo else None,
                      subtotal, shipping_fee, discount_amount, total_amount,
                      payment, order_status, order_str, order_str))

                kafka_items = []
                for item in items_to_buy:
                    cur.execute("""
                        INSERT INTO order_items
                            (order_id, product_id, quantity, unit_price, unit_cost, item_total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (order_id, item["id"], item["qty"], item["price"], item["cost"], item["item_total"]))

                    cur.execute(
                        "UPDATE inventory SET stock_quantity = stock_quantity - %s WHERE product_id = %s",
                        (item["qty"], item["id"])
                    )
                    cur.execute("""
                        INSERT INTO inventory_logs
                            (product_id, order_id, transaction_type, quantity_changed, created_at, note)
                        VALUES (%s, %s, 'OUT', %s, %s, 'Sell')
                    """, (item["id"], order_id, item["qty"], order_str))

                    # Low-stock alert
                    cur.execute("SELECT stock_quantity FROM inventory WHERE product_id = %s", (item["id"],))
                    new_stock = cur.fetchone()[0]
                    if new_stock < 10:
                        send_sim(TOPIC_INVENTORY, "low_stock_alert", {
                            "product_id": item["id"], "product_name": item["name"],
                            "stock_remaining": new_stock,
                        }, ts=order_str)

                    kafka_items.append({
                        "product_id": item["id"], "product_name": item["name"],
                        "quantity": item["qty"], "unit_price": item["price"],
                        "unit_cost": item["cost"],
                    })

                    send_sim(TOPIC_INVENTORY, "stock_updated_out", {
                        "product_id": item["id"], "product_name": item["name"],
                        "quantity_sold": item["qty"],
                        "order_id": order_id,
                        "stock_remaining": new_stock,
                    }, ts=order_str)

            conn.commit()
            db_success = True

        except Exception:
            conn.rollback()
            db_success = False

        if not db_success:
            continue

        # ── 5. KAFKA: Order Events ────────────────────────────────
        send_sim(TOPIC_ORDER, "order_created", {
            "order_id":       order_id,
            "subtotal":       subtotal,
            "shipping_fee":   shipping_fee,
            "discount_amount": discount_amount,
            "total_amount":   total_amount,
            "payment_method": payment,
            "promo_id":       promo["promo_id"] if promo else None,
            "promo_code":     promo["promo_code"] if promo else None,
            "order_status":   order_status,
            "items":          kafka_items,
        }, ts=order_str)

        if order_status not in ('Cancelled', 'Pending'):
            paid_dt  = order_dt + timedelta(minutes=random.randint(1, 30))
            paid_str = paid_dt.strftime('%Y-%m-%d %H:%M:%S')
            send_sim(TOPIC_ORDER, "order_paid", {
                "order_id":       order_id,
                "payment_method": payment,
                "order_status":   order_status,
                "subtotal":       subtotal,
                "shipping_fee":   shipping_fee,
                "discount_amount": discount_amount,
                "total_amount":   total_amount,
                "promo_id":       promo["promo_id"] if promo else None,     
                "promo_code":     promo["promo_code"] if promo else None,
            }, ts=paid_str)

        if order_status == 'Cancelled':
            send_sim(TOPIC_ORDER, "order_cancelled", {
                "order_id":       order_id,
                "payment_method": payment,
                "order_status":   "Cancelled",
                "subtotal":       subtotal,
                "shipping_fee":   shipping_fee,
                "discount_amount": discount_amount,
                "total_amount":   total_amount,
                "promo_id":       promo["promo_id"] if promo else None,     
                "promo_code":     promo["promo_code"] if promo else None,
                "reason":         random.choice(["Changed mind", "Found cheaper", "Wrong item"]),
            }, ts=order_str)

        # ── 6. KAFKA: Returned orders + inventory_log RETURN ──────
        if order_status == 'Returned':
            return_dt  = order_dt + timedelta(days=random.randint(1, 7))
            return_str = return_dt.strftime('%Y-%m-%d %H:%M:%S')
            return_item = random.choice(kafka_items)
            return_qty  = random.randint(1, return_item["quantity"])

            send_sim(TOPIC_ORDER, "order_returned", {
                "order_id":          order_id,
                "product_id":        return_item["product_id"],
                "quantity_returned": return_qty,
                "payment_method":    payment,
                "order_status":      "Returned",
                "subtotal":          subtotal,
                "shipping_fee":      shipping_fee,
                "discount_amount":   discount_amount,
                "total_amount":      total_amount,
                "reason":            random.choice(["Defective", "Wrong size", "Not as described", "Changed mind"]),
            }, ts=return_str)

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE inventory SET stock_quantity = stock_quantity + %s WHERE product_id = %s",
                        (return_qty, return_item["product_id"])
                    )
                    cur.execute("""
                        INSERT INTO inventory_logs
                            (product_id, order_id, transaction_type, quantity_changed, created_at, note)
                        VALUES (%s, %s, 'RETURN', %s, %s, 'Return')
                    """, (return_item["product_id"], order_id, return_qty, return_str))
                conn.commit()

                cur.execute("SELECT stock_quantity FROM inventory WHERE product_id = %s", (return_item["product_id"],))
                new_stock_after_return = cur.fetchone()[0]

                send_sim(TOPIC_INVENTORY, "stock_updated_in", {
                    "product_id": return_item["product_id"],
                    "product_name": return_item["product_name"],
                    "quantity_returned": return_qty, "order_id": order_id,
                    "stock_remaining": new_stock_after_return,
                }, ts=return_str)
            except:
                conn.rollback()

        # ── 7. KAFKA: Reviews (chỉ Delivered) ────────────────────
        if order_status == 'Delivered' and random.random() < 0.5:
            for item in kafka_items:
                rating  = random.choices([1, 2, 3, 4, 5], weights=[3, 5, 12, 40, 40])[0]
                comment_pool = [
                    "Sản phẩm tốt, giao hàng nhanh!", "Hàng y mô tả, rất hài lòng.",
                    "Chất lượng tạm ổn.", "Sẽ mua lại lần sau.", "Đóng gói cẩn thận.",
                    "Hơi chậm nhưng hàng tốt.", "Không như kỳ vọng lắm.", "Tuyệt vời!",
                ]
                comment  = random.choice(comment_pool)
                review_dt  = order_dt + timedelta(days=random.randint(1, 14))
                review_str = review_dt.strftime('%Y-%m-%d %H:%M:%S')

                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO reviews
                                (product_id, user_id, rating, comment, review_date)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (item["product_id"], sim_user_id, rating, comment, review_str))
                    conn.commit()
                    send_sim(TOPIC_REVIEW, "review_submitted", {
                        "product_id": item["product_id"],
                        "rating": rating,
                        "comment_length": len(comment),
                    }, ts=review_str)
                except:
                    conn.rollback()

    producer.flush(timeout=5.0)
    return True


def simulate_realtime_traffic(num_users: int):
    """
    Giả lập dữ liệu thời gian thực (Real-time).
    Tất cả timestamp đều diễn ra xung quanh thời điểm NGAY BÂY GIỜ.
    """
    conn = init_connection()
    producer = get_kafka_producer()
    if not producer or not conn:
        return False

    VN_LAST_NAMES = ['Nguyễn', 'Trần', 'Lê', 'Phạm', 'Hoàng', 'Huỳnh', 'Phan', 'Vũ', 'Võ', 'Đặng', 'Bùi', 'Đỗ', 'Hồ', 'Ngô', 'Dương', 'Lý']
    VN_MIDDLE_NAMES = ['Văn', 'Thị', 'Hữu', 'Ngọc', 'Minh', 'Thanh', 'Gia', 'Bảo', 'Hoài', 'Xuân', 'Thu', 'Đức', 'Tài', 'Thành']
    VN_FIRST_NAMES = ['Anh', 'Tuấn', 'Hùng', 'Dũng', 'Linh', 'Lan', 'Trang', 'Hương', 'Hà', 'Khang', 'Khôi', 'Phúc', 'Tâm', 'An', 'Bình', 'Hân', 'Thảo', 'Nhi']
    VN_PROVINCES = ['Hà Nội', 'TP. Hồ Chí Minh', 'Đà Nẵng', 'Hải Phòng', 'Cần Thơ', 'Đồng Nai', 'Bình Dương', 'Bà Rịa - Vũng Tàu', 'Khánh Hòa', 'Lâm Đồng', 'Thừa Thiên Huế', 'Quảng Ninh', 'Nghệ An', 'Thanh Hóa', 'Bắc Ninh', 'Cà Mau', 'Kiên Giang']
    PAYMENT_METHODS = ['COD', 'Credit Card', 'E-Wallet', 'Bank Transfer']
    DEVICES = ['Mobile', 'Desktop', 'Tablet']
    OS_LIST = ['iOS', 'Android', 'Windows', 'MacOS']
    SEARCH_TERMS = ['iPhone', 'laptop gaming', 'tai nghe chống ồn', 'Samsung', 'MacBook', 'tablet học online', 'Xiaomi', 'Sony']

    # Lấy toàn bộ promotions hợp lệ để dùng trong sim
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT promo_id, promo_code, discount_percent FROM promotions WHERE is_active = TRUE")
            all_promos = [dict(r) for r in cur.fetchall()]
    except:
        all_promos = []

    current_products = [p for p in PRODUCTS if p.get('stock', 0) > 0]
    if not current_products:
        return False

    for _ in range(num_users):
        # 🌟 REAL-TIME LOGIC: Khách vào web lùi lại từ 0-3 phút so với hiện tại
        user_reg_dt = datetime.now() - timedelta(minutes=random.randint(0, 3), seconds=random.randint(0, 59))
        user_reg_str = user_reg_dt.strftime('%Y-%m-%d %H:%M:%S')

        # ── 1. TẠO KHÁCH ẢO ─────────────────────────────────────
        last_name = random.choice(VN_LAST_NAMES)
        middle_name = random.choice(VN_MIDDLE_NAMES)
        first_name = random.choice(VN_FIRST_NAMES)
        full_name = f"{last_name} {middle_name} {first_name}"
        clean_name = remove_accents(f"{first_name}{last_name}").lower().replace(" ", "")
        username = f"{clean_name}{random.randint(10, 9999)}"
        email = f"{username}@gmail.com"
        password = ''.join(random.choices(string.ascii_letters + string.digits, k=8))
        sim_user_id = f"U{uuid.uuid4().hex[:6].upper()}"
        phone       = f"09{random.randint(10000000, 99999999)}"
        gender      = random.choice(['Male', 'Female'])
        location    = random.choice(VN_PROVINCES)
        session_id  = str(uuid.uuid4())
        device      = random.choice(DEVICES)
        os_sys      = random.choice(OS_LIST)

        sim_user_obj = {"user_id": sim_user_id, "name": full_name}

        def send_sim(topic: str, event_type: str, payload: dict, ts: str = user_reg_str):
            if topic == TOPIC_PAGE_VIEW and "page_name" not in payload:
                if event_type == "product_viewed":
                    payload["page_name"] = "product_detail"
                elif event_type in ["search_performed", "category_filtered"]:
                    payload["page_name"] = "products"
                else:
                    payload["page_name"] = "home"
            event = {
                "event_id":   str(uuid.uuid4()),
                "event_type": event_type,
                "timestamp":  ts,
                "session_id": session_id,
                "user_id":    sim_user_id,
                "user_name":  full_name,
                "location":   location,
                "device_type": device,
                "os":          os_sys,
                **payload,
            }
            producer.send(topic, value=event)

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO users
                        (user_id, username, password, full_name, email, phone, gender, location, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (sim_user_id, username, "sim123", full_name, email, phone, gender, location, user_reg_str))
            conn.commit()
        except:
            conn.rollback()
            continue

        # ── 2. KAFKA: User Registered ─────────────────────────────
        send_sim(TOPIC_USER, "user_registered", {"username": username, "gender": gender})

        # ── 3. KAFKA: Page Views & Search (FULL USER JOURNEY) ─────────
        send_sim(TOPIC_PAGE_VIEW, "page_viewed", {
            "page_name": "home",
            "trigger": "direct_visit"
        }, ts=user_reg_str)

        if random.random() < 0.6:
            search_term = random.choice(SEARCH_TERMS)
            send_sim(TOPIC_PAGE_VIEW, "search_performed", {
                "page_name": "search_results",
                "search_term": search_term,
                "results_count": random.randint(3, 15),
                "trigger": "search_bar"
            }, ts=user_reg_str)
        elif random.random() < 0.5:
            cat = random.choice(CATEGORIES[1:]) if len(CATEGORIES) > 1 else "Smartphone"
            send_sim(TOPIC_PAGE_VIEW, "category_filtered", {
                "page_name": "category_list",
                "category": cat,
                "trigger": "sidebar_filter"
            }, ts=user_reg_str)

        browsed = random.sample(current_products, min(random.randint(5, 12), len(current_products)))
        for bp in browsed:
            # 🌟 REAL-TIME LOGIC: Xem sản phẩm sau vài giây
            view_ts = (user_reg_dt + timedelta(seconds=random.randint(5, 60))).strftime('%Y-%m-%d %H:%M:%S')
            send_sim(TOPIC_PAGE_VIEW, "product_viewed", {
                "page_name": "product_detail",
                "product_id": bp["id"], 
                "product_name": bp["name"],
                "category": bp["category"], 
                "brand": bp["brand"], 
                "price": bp["price"],
                "trigger": "click_product_card"
            }, ts=view_ts)

        # 🌟 REAL-TIME LOGIC: Vào giỏ hàng sau vài chục giây
        cart_view_ts = (user_reg_dt + timedelta(seconds=random.randint(30, 90))).strftime('%Y-%m-%d %H:%M:%S')
        send_sim(TOPIC_PAGE_VIEW, "page_viewed", {
            "page_name": "cart",
            "trigger": "click_cart_icon"
        }, ts=cart_view_ts)

        # ── 4. CHỌN SẢN PHẨM ĐỂ MUA ─────────────────────────────
        num_items = random.randint(1, 3)
        chosen_products = random.sample(current_products, min(num_items, len(current_products)))

        # 🌟 REAL-TIME LOGIC: Đặt hàng diễn ra nhanh trong 1 đến 3 phút
        order_offset_seconds = random.randint(30, 180)
        order_dt  = user_reg_dt + timedelta(seconds=order_offset_seconds)
        order_str = order_dt.strftime('%Y-%m-%d %H:%M:%S')

        order_id = f"ORD_{uuid.uuid4().hex[:8].upper()}"
        subtotal = 0
        db_success = False

        promo = random.choice(all_promos) if all_promos and random.random() < 0.30 else None
        if promo:
            send_sim(TOPIC_PROMO, "promo_applied", {
                "promo_id": promo["promo_id"],
                "promo_code": promo["promo_code"],
                "discount_percent": float(promo["discount_percent"]),
            }, ts=order_str)

        try:
            with conn.cursor() as cur:
                items_to_buy = []
                for p in chosen_products:
                    cur.execute("SELECT stock_quantity FROM inventory WHERE product_id = %s FOR UPDATE", (p["id"],))
                    res = cur.fetchone()
                    if res and res[0] > 0:
                        actual_qty = min(random.randint(1, 2), res[0])
                        item_total = p["price"] * actual_qty
                        items_to_buy.append({
                            "id": p["id"], "name": p["name"], "price": p["price"],
                            "cost": p["cost"], "qty": actual_qty, "item_total": item_total,
                            "brand": p["brand"], "category": p["category"],
                        })
                        subtotal += item_total

                        send_sim(TOPIC_CART, "item_added", {
                            "product_id": p["id"], "product_name": p["name"],
                            "brand": p["brand"], "category": p["category"],
                            "unit_price": p["price"], "quantity": actual_qty,
                            "cart_quantity_after": actual_qty,                 
                            "old_quantity": 0,                                 
                            "new_quantity": actual_qty,
                        }, ts=order_str)

                        send_sim(TOPIC_PAGE_VIEW, "product_viewed", {
                            "page_name": "product_detail",
                            "product_id": p["id"], "product_name": p["name"],
                            "category": p["category"], "brand": p["brand"], "price": p["price"],
                            "trigger": "add_to_cart",
                        }, ts=order_str)

                if not items_to_buy:
                    conn.rollback()
                    continue

                shipping_fee    = random.choice([0, 15000, 30000])
                discount_amount = int(subtotal * promo["discount_percent"] / 100) if promo else 0
                total_amount    = subtotal - discount_amount + shipping_fee
                payment         = random.choice(PAYMENT_METHODS)

                order_status = random.choices(
                    ['Delivered', 'Processing', 'Pending', 'Cancelled', 'Returned'],
                    weights=[70, 10, 10, 5, 5]
                )[0]

                cur.execute("""
                    INSERT INTO orders
                        (order_id, user_id, promo_id, subtotal, shipping_fee,
                         discount_amount, total_amount, payment_method, order_status,
                         order_date, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (order_id, sim_user_id, promo["promo_id"] if promo else None,
                      subtotal, shipping_fee, discount_amount, total_amount,
                      payment, order_status, order_str, order_str))

                kafka_items = []
                for item in items_to_buy:
                    cur.execute("""
                        INSERT INTO order_items
                            (order_id, product_id, quantity, unit_price, unit_cost, item_total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (order_id, item["id"], item["qty"], item["price"], item["cost"], item["item_total"]))

                    cur.execute(
                        "UPDATE inventory SET stock_quantity = stock_quantity - %s WHERE product_id = %s",
                        (item["qty"], item["id"])
                    )
                    cur.execute("""
                        INSERT INTO inventory_logs
                            (product_id, order_id, transaction_type, quantity_changed, created_at, note)
                        VALUES (%s, %s, 'OUT', %s, %s, 'Sell')
                    """, (item["id"], order_id, item["qty"], order_str))

                    cur.execute("SELECT stock_quantity FROM inventory WHERE product_id = %s", (item["id"],))
                    new_stock = cur.fetchone()[0]
                    if new_stock < 10:
                        send_sim(TOPIC_INVENTORY, "low_stock_alert", {
                            "product_id": item["id"], "product_name": item["name"],
                            "stock_remaining": new_stock,
                        }, ts=order_str)

                    kafka_items.append({
                        "product_id": item["id"], "product_name": item["name"],
                        "quantity": item["qty"], "unit_price": item["price"],
                        "unit_cost": item["cost"],
                    })

                    send_sim(TOPIC_INVENTORY, "stock_updated_out", {
                        "product_id": item["id"], "product_name": item["name"],
                        "quantity_sold": item["qty"],
                        "order_id": order_id,
                        "stock_remaining": new_stock,
                    }, ts=order_str)

            conn.commit()
            db_success = True

        except Exception:
            conn.rollback()
            db_success = False

        if not db_success:
            continue

        # ── 5. KAFKA: Order Events ────────────────────────────────
        send_sim(TOPIC_ORDER, "order_created", {
            "order_id":       order_id,
            "subtotal":       subtotal,
            "shipping_fee":   shipping_fee,
            "discount_amount": discount_amount,
            "total_amount":   total_amount,
            "payment_method": payment,
            "promo_id":       promo["promo_id"] if promo else None,
            "promo_code":     promo["promo_code"] if promo else None,
            "order_status":   order_status,
            "items":          kafka_items,
        }, ts=order_str)

        if order_status not in ('Cancelled', 'Pending'):
            # 🌟 REAL-TIME LOGIC: Thanh toán ngay sau 10 đến 45 giây
            paid_dt  = order_dt + timedelta(seconds=random.randint(10, 45))
            paid_str = paid_dt.strftime('%Y-%m-%d %H:%M:%S')
            send_sim(TOPIC_ORDER, "order_paid", {
                "order_id":       order_id,
                "payment_method": payment,
                "order_status":   order_status,
                "subtotal":       subtotal,
                "shipping_fee":   shipping_fee,
                "discount_amount": discount_amount,
                "total_amount":   total_amount,
                "promo_id":       promo["promo_id"] if promo else None,     
                "promo_code":     promo["promo_code"] if promo else None,
            }, ts=paid_str)

        if order_status == 'Cancelled':
            send_sim(TOPIC_ORDER, "order_cancelled", {
                "order_id":       order_id,
                "payment_method": payment,
                "order_status":   "Cancelled",
                "subtotal":       subtotal,
                "shipping_fee":   shipping_fee,
                "discount_amount": discount_amount,
                "total_amount":   total_amount,
                "promo_id":       promo["promo_id"] if promo else None,     
                "promo_code":     promo["promo_code"] if promo else None,
                "reason":         random.choice(["Changed mind", "Found cheaper", "Wrong item"]),
            }, ts=order_str)

        # ── 6. KAFKA: Returned orders + inventory_log RETURN ──────
        if order_status == 'Returned':
            # 🌟 REAL-TIME LOGIC: Hoàn hàng sau 1 đến 5 phút
            return_dt  = order_dt + timedelta(minutes=random.randint(1, 5))
            return_str = return_dt.strftime('%Y-%m-%d %H:%M:%S')
            return_item = random.choice(kafka_items)
            return_qty  = random.randint(1, return_item["quantity"])

            send_sim(TOPIC_ORDER, "order_returned", {
                "order_id":          order_id,
                "product_id":        return_item["product_id"],
                "quantity_returned": return_qty,
                "payment_method":    payment,
                "order_status":      "Returned",
                "subtotal":          subtotal,
                "shipping_fee":      shipping_fee,
                "discount_amount":   discount_amount,
                "total_amount":      total_amount,
                "reason":            random.choice(["Defective", "Wrong size", "Not as described", "Changed mind"]),
            }, ts=return_str)

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE inventory SET stock_quantity = stock_quantity + %s WHERE product_id = %s",
                        (return_qty, return_item["product_id"])
                    )
                    cur.execute("""
                        INSERT INTO inventory_logs
                            (product_id, order_id, transaction_type, quantity_changed, created_at, note)
                        VALUES (%s, %s, 'RETURN', %s, %s, 'Return')
                    """, (return_item["product_id"], order_id, return_qty, return_str))
                conn.commit()

                cur.execute("SELECT stock_quantity FROM inventory WHERE product_id = %s", (return_item["product_id"],))
                new_stock_after_return = cur.fetchone()[0]

                send_sim(TOPIC_INVENTORY, "stock_updated_in", {
                    "product_id": return_item["product_id"],
                    "product_name": return_item["product_name"],
                    "quantity_returned": return_qty, "order_id": order_id,
                    "stock_remaining": new_stock_after_return,
                }, ts=return_str)
            except:
                conn.rollback()

        # ── 7. KAFKA: Reviews (chỉ Delivered) ────────────────────
        if order_status == 'Delivered' and random.random() < 0.5:
            for item in kafka_items:
                rating  = random.choices([1, 2, 3, 4, 5], weights=[3, 5, 12, 40, 40])[0]
                comment_pool = [
                    "Sản phẩm tốt, giao hàng nhanh!", "Hàng y mô tả, rất hài lòng.",
                    "Chất lượng tạm ổn.", "Sẽ mua lại lần sau.", "Đóng gói cẩn thận.",
                    "Hơi chậm nhưng hàng tốt.", "Không như kỳ vọng lắm.", "Tuyệt vời!",
                ]
                comment  = random.choice(comment_pool)
                # 🌟 REAL-TIME LOGIC: Viết đánh giá sau 2 đến 10 phút
                review_dt  = order_dt + timedelta(minutes=random.randint(2, 10))
                review_str = review_dt.strftime('%Y-%m-%d %H:%M:%S')

                try:
                    with conn.cursor() as cur:
                        cur.execute("""
                            INSERT INTO reviews
                                (product_id, user_id, rating, comment, review_date)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (item["product_id"], sim_user_id, rating, comment, review_str))
                    conn.commit()
                    send_sim(TOPIC_REVIEW, "review_submitted", {
                        "product_id": item["product_id"],
                        "rating": rating,
                        "comment_length": len(comment),
                    }, ts=review_str)
                except:
                    conn.rollback()

    producer.flush(timeout=5.0)
    return True
# ══════════════════════════════════════════════════════════════════
# UI COMPONENTS
# ══════════════════════════════════════════════════════════════════
def render_navbar():
    badge      = f'<span class="cart-badge">{cart_count()}</span>' if cart_count() > 0 else ""
    user_label = st.session_state.user["name"].split()[0] if st.session_state.logged_in else ""
    html = (
        f'<div class="navbar">'
        f'<div class="navbar-brand">✦ KHOI STORE</div>'
        f'<div class="navbar-links"><span class="nav-link">Sản phẩm mới</span><span class="nav-link">Danh mục</span></div>'
        f'<div style="display:flex;align-items:center;gap:1rem;">'
    )
    if user_label:
        html += f'<span style="color:var(--accent);font-weight:500;">👤 {user_label}</span>'
    html += f'<span style="color:var(--cream);">🛒 Giỏ hàng {badge}</span></div></div>'
    st.markdown(html, unsafe_allow_html=True)

def render_notification():
    if st.session_state.notification:
        st.markdown(f'<div class="toast">{st.session_state.notification}</div>', unsafe_allow_html=True)
        st.session_state.notification = None

def render_product_card(p):
    tag_html = (
        f'<div style="position:relative;"><span style="position:absolute;top:-8px;right:0;'
        f'background:var(--accent);color:white;font-size:0.65rem;padding:2px 8px;border-radius:20px;'
        f'font-weight:600;z-index:10;">{p["tag"]}</span></div>'
    ) if p.get("tag") else ""

    current_dir  = os.path.dirname(os.path.abspath(__file__))
    img_filename = p.get("image_url") or "null.jpg"
    img_path     = os.path.join(current_dir, "image", img_filename)

    img_b64 = ""
    if os.path.exists(img_path):
        try:
            with open(img_path, "rb") as f:
                img_b64 = base64.b64encode(f.read()).decode()
        except:
            pass

    ext = "jpeg" if img_filename.lower().endswith("jpg") else "png"
    if img_b64:
        img_html = (
            f'<div class="product-img" style="background-image: url(\'data:image/{ext};base64,{img_b64}\');'
            f'background-size: contain; background-position: center; background-repeat: no-repeat;'
            f'background-color: white; height: 200px; width: 100%; border-radius: 16px 16px 0 0;"></div>'
        )
    else:
        img_html = (
            f'<div class="product-img" style="height: 200px; background-color: var(--cream); color: #dc3545;'
            f'display:flex; align-items:center; justify-content:center; text-align:center;'
            f'border-radius: 16px 16px 0 0;">📷 Thiếu file:<br><b>{img_filename}</b></div>'
        )

    html_content = (
        f'<div class="product-card" style="padding-bottom: 0px; border-bottom: none;'
        f'border-bottom-left-radius: 0; border-bottom-right-radius: 0; height: 100%;">'
        f'{tag_html}{img_html}'
        f'<div class="product-body" style="height: 120px;">'
        f'<div style="display:flex; justify-content:space-between; margin-bottom: 5px;">'
        f'<div style="color:var(--accent);font-size:0.7rem;text-transform:uppercase;font-weight:bold;">'
        f'{p["brand"]} | {p["category"]}</div>'
        f'<div style="color:var(--muted);font-size:0.7rem;font-weight:bold;">Kho: {p.get("stock", 0)}</div>'
        f'</div>'
        f'<div class="product-name" style="font-size: 1rem; overflow: hidden; text-overflow: ellipsis;'
        f'display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;">{p["name"]}</div>'
        f'<div class="product-price" style="margin-top: 8px;">{p["price"]:,} đ</div>'
        f'</div></div>'
    )
    st.markdown(html_content, unsafe_allow_html=True)

    # if st.button("Thêm vào giỏ", key=f"atc_{p['id']}", use_container_width=True):
    #     add_to_cart(p["id"])
    #     # Fire product_viewed event on add-to-cart (user clearly interested)
    #     send_kafka_event(TOPIC_PAGE_VIEW, "product_viewed", {
    #         "product_id": p["id"], "product_name": p["name"],
    #         "category": p["category"], "brand": p["brand"], "price": p["price"],
    #         "trigger": "add_to_cart",
    #     })
    #     st.rerun()
    # Bỏ đoạn cũ đi (if st.button("Thêm vào giỏ"...)) và thay bằng đoạn này:
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Xem chi tiết sản phẩm", key=f"view_{p['id']}", use_container_width=True):
            # Bắn sự kiện xem sản phẩm lên Kafka
            send_kafka_event(TOPIC_PAGE_VIEW, "product_viewed", {
                "page_name": "products",
                "product_id": p["id"], "product_name": p["name"],
                "category": p["category"], "brand": p["brand"], "price": p["price"],
                "trigger": "click_view_button",
            })
            st.session_state.notification = f"Bạn đang xem thông tin {p['name']}!"
            st.rerun()
    with col2:
        if st.button("🛒 Thêm", key=f"atc_{p['id']}", use_container_width=True):
            add_to_cart(p["id"])
            # Vừa thêm vào giỏ vừa ghi nhận là đã xem
            send_kafka_event(TOPIC_PAGE_VIEW, "product_viewed", {
                "page_name": "products",
                "product_id": p["id"], "product_name": p["name"],
                "category": p["category"], "brand": p["brand"], "price": p["price"],
                "trigger": "add_to_cart",
            })
            st.rerun()

# ══════════════════════════════════════════════════════════════════
# PAGES
# ══════════════════════════════════════════════════════════════════
def page_home():
    st.markdown(
        '<div class="hero">'
        '<div class="hero-eyebrow">Bộ Sưu Tập 2025</div>'
        '<div class="hero-title">Đẳng Cấp & Khác Biệt</div>'
        '</div>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="section-heading">Sản Phẩm Nổi Bật</div><div class="divider"></div>', unsafe_allow_html=True)
    cols = st.columns(4)
    for i, p in enumerate(PRODUCTS[:4]):
        with cols[i]:
            render_product_card(p)

# def page_products():
#     st.markdown('<div class="section-heading">Tất Cả Sản Phẩm</div><div class="divider"></div>', unsafe_allow_html=True)

#     # Category filter
#     selected_cat = st.selectbox("Lọc theo danh mục:", CATEGORIES, label_visibility="collapsed")
#     filtered = PRODUCTS if selected_cat == "All" else [p for p in PRODUCTS if p["category"] == selected_cat]
#     if selected_cat != "All":
#         send_kafka_event(TOPIC_PAGE_VIEW, "category_filtered", {"category": selected_cat})

#     cols = st.columns(4)
#     for i, p in enumerate(filtered):
#         with cols[i % 4]:
#             render_product_card(p)

def page_products():
    st.markdown('<div class="section-heading">Tất Cả Sản Phẩm</div><div class="divider"></div>', unsafe_allow_html=True)

    # ── 1. Thanh tìm kiếm ──
    search_query = st.text_input("🔍 Tìm kiếm sản phẩm:", placeholder="Nhập tên, thương hiệu (VD: iPhone)...")
    if search_query and search_query != st.session_state.get("last_search", ""):
        st.session_state.last_search = search_query
        filtered_by_search = [p for p in PRODUCTS if search_query.lower() in p["name"].lower() or search_query.lower() in p["brand"].lower()]
        
        # Bắn sự kiện tìm kiếm lên Kafka
        send_kafka_event(TOPIC_PAGE_VIEW, "search_performed", {
            "search_term": search_query,
            "results_count": len(filtered_by_search)
        })
    else:
        filtered_by_search = PRODUCTS

    # ── 2. Lọc theo danh mục ──
    selected_cat = st.selectbox("Lọc theo danh mục:", CATEGORIES, label_visibility="collapsed")
    filtered = filtered_by_search if selected_cat == "All" else [p for p in filtered_by_search if p["category"] == selected_cat]
    
    if selected_cat != "All" and selected_cat != st.session_state.get("last_category", ""):
        st.session_state.last_category = selected_cat
        # Bắn sự kiện lọc danh mục lên Kafka
        send_kafka_event(TOPIC_PAGE_VIEW, "category_filtered", {"category": selected_cat})

    # ── 3. Hiển thị sản phẩm ──
    cols = st.columns(4)
    for i, p in enumerate(filtered):
        with cols[i % 4]:
            render_product_card(p)

def page_cart():
    st.markdown('<div class="section-heading">Giỏ Hàng Của Bạn</div><div class="divider"></div>', unsafe_allow_html=True)
    if not st.session_state.cart:
        st.info("Giỏ hàng đang trống. Hãy thêm vài món đồ vào nhé!")
        return

    left, right = st.columns([3, 2], gap="large")

    with left:
        for pid, qty in list(st.session_state.cart.items()):
            p = next((x for x in PRODUCTS if x["id"] == pid), None)
            if not p:
                continue
            c1, c2, c3 = st.columns([3, 1, 1])
            with c1:
                st.markdown(f"**📦 {p['name']}** <br> {p['price']:,} đ", unsafe_allow_html=True)
            with c2:
                max_stock = max(p.get("stock", 1), 1)
                new_qty = st.number_input("SL", min_value=1, max_value=max_stock, value=qty,
                                          key=f"qty_{pid}", label_visibility="collapsed")
                if new_qty != qty:
                    update_cart_qty(pid, new_qty)
            with c3:
                if st.button("Xóa", key=f"rm_{pid}"):
                    remove_from_cart(pid)
                    st.rerun()
            st.markdown("---")

    with right:
        subtotal = cart_subtotal()
        shipping_fee = 15000 if subtotal < 10_000_000 else 0

        # ── Promo code input ──
        promo = st.session_state.applied_promo
        if not promo:
            promo_col1, promo_col2 = st.columns([3, 1])
            with promo_col1:
                promo_input = st.text_input("Mã giảm giá:", placeholder="Nhập mã...", label_visibility="collapsed")
            with promo_col2:
                if st.button("Áp dụng"):
                    if promo_input:
                        ok, msg = apply_promo_code(promo_input)
                        if ok:
                            st.success(msg)
                        else:
                            st.error(msg)
                        st.rerun()
        else:
            st.success(f"✅ Mã **{promo['promo_code']}** — -{promo['discount_percent']}%")
            if st.button("Xóa mã"):
                remove_promo()
                st.rerun()

        discount_amount, discounted_subtotal = apply_discount(subtotal, promo)

        st.markdown(f"Tạm tính: **{subtotal:,} đ**")
        if discount_amount > 0:
            st.markdown(f"Giảm giá: **-{discount_amount:,} đ**")
        st.markdown(f"Phí giao hàng: **{shipping_fee:,} đ**")
        st.markdown(f"### Tổng cộng: **{(discounted_subtotal + shipping_fee):,} đ**")

        payment_options = ["COD", "Credit Card", "E-Wallet", "Bank Transfer"]
        payment_choice = st.selectbox("Phương thức thanh toán:", payment_options)

        btn_label = "Đặt Hàng" if st.session_state.logged_in else "Đăng Nhập Để Mua Hàng"
        if st.button(btn_label, use_container_width=True):
            if not st.session_state.logged_in:
                navigate("login")
            else:
                checkout_order(subtotal, payment_choice)

def page_login():
    _, center, _ = st.columns([1, 2, 1])
    with center:
        tab1, tab2 = st.tabs(["Đăng Nhập", "Đăng Ký"])
        with tab1:
            st.markdown("### Mừng trở lại!")
            username = st.text_input("Tên đăng nhập (Thử khoi/admin)")
            password = st.text_input("Mật khẩu (Thử 123/admin)", type="password")
            if st.button("Đăng Nhập", use_container_width=True):
                send_kafka_event(TOPIC_USER, "user_login_attempt", {"username": username})
                if login(username, password):
                    navigate("home")
                else:
                    st.error("Sai tài khoản hoặc mật khẩu!")
        with tab2:
            st.markdown("### Tạo tài khoản mới")
            new_name   = st.text_input("Họ và Tên")
            col1, col2 = st.columns(2)
            with col1: new_phone  = st.text_input("Số điện thoại")
            with col2: new_gender = st.selectbox("Giới tính", ["Male", "Female", "Other"])
            new_email = st.text_input("Email")
            new_loc   = st.text_input("Tỉnh / Thành phố")
            new_user  = st.text_input("Tên đăng nhập")
            new_pass  = st.text_input("Mật khẩu", type="password")
            if st.button("Đăng Ký", use_container_width=True):
                if register(new_name, new_email, new_phone, new_gender, new_loc, new_user, new_pass):
                    st.success("Đăng ký thành công! Hãy đăng nhập.")
                else:
                    st.error("Tên đăng nhập / Email đã tồn tại hoặc có lỗi.")

def page_profile():
    if not st.session_state.logged_in:
        navigate("login")
        return
    u = st.session_state.user

    if u["username"] == "admin":
        st.markdown("### 👑 TRANG QUẢN TRỊ ADMIN")
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

        # ── Nhập hàng thủ công ──
        with st.form("restock_form"):
            st.markdown("**📦 Nhập Hàng Vào Kho**")
            product_options = {
                p["id"]: f"[{p.get('brand', '')}] {p['name']} (Tồn kho: {p.get('stock', 0)})"
                for p in PRODUCTS
            }
            selected_pid = st.selectbox("Chọn mặt hàng:", options=list(product_options.keys()),
                                         format_func=lambda x: product_options[x])
            restock_qty = st.number_input("Số lượng nhập:", min_value=1, value=10, step=5)
            if st.form_submit_button("Xác nhận Nhập Kho", use_container_width=True):
                if restock_item(selected_pid, restock_qty):
                    st.success(f"✅ Đã nhập {restock_qty} sản phẩm!")
                    time.sleep(1)
                    st.rerun()

        st.markdown('<div class="divider" style="margin-top: 2rem;"></div>', unsafe_allow_html=True)

        # ── Simulator ──
        # st.markdown("### 🤖 Công cụ Giả Lập Dữ liệu Lịch sử & Traffic")
        # with st.form("simulator_form"):
        #     sim_qty = st.number_input("Số lượng Khách hàng ảo:", min_value=1, max_value=1000, value=20, step=10)
        #     if st.form_submit_button("🚀 Bắn Data Lịch sử (100 ngày) lên Kafka & DB", use_container_width=True):
        #         with st.spinner(f"Đang sinh dữ liệu lịch sử cho {sim_qty} khách hàng..."):
        #             if simulate_traffic(sim_qty):
        #                 st.success("✅ HOÀN TẤT! Dữ liệu đã được ghi vào DB và Kafka.")
        #             else:
        #                 st.error("Lỗi kết nối Kafka / DB.")
        
        st.markdown("### 🤖 Công cụ Giả Lập Dữ liệu")
        with st.form("simulator_form"):
            sim_qty = st.number_input("Số lượng Khách hàng ảo:", min_value=1, max_value=1000, value=20, step=10)
            
            # Giao diện chia 2 cột cho 2 nút bấm
            col1, col2 = st.columns(2)
            with col1:
                btn_history = st.form_submit_button("🕰️ Bắn Data Lịch sử", use_container_width=True)
            with col2:
                btn_realtime = st.form_submit_button("🔥 Bắn Data Real-time", use_container_width=True)
                
            if btn_history:
                with st.spinner(f"Đang sinh dữ liệu Lịch sử cho {sim_qty} khách hàng..."):
                    if simulate_traffic(sim_qty):
                        st.success("✅ HOÀN TẤT! Dữ liệu LỊCH SỬ đã được ghi.")
                    else:
                        st.error("Lỗi kết nối Kafka / DB.")
            
            if btn_realtime:
                with st.spinner(f"Đang sinh dữ liệu Real-time cho {sim_qty} khách hàng..."):
                    if simulate_realtime_traffic(sim_qty):
                        st.success("✅ HOÀN TẤT! Dữ liệu REAL-TIME đã bay lên Kafka.")
                    else:
                        st.error("Lỗi kết nối Kafka / DB.")

        st.markdown('<div class="divider" style="margin-top: 2rem;"></div>', unsafe_allow_html=True)

        # ── Auto restock ──
        if st.button("🔄 Tự động Quét & Nhập Kho", use_container_width=True):
            with st.spinner("Quét kho..."):
                restocked = auto_restock(15, 50)
                if restocked > 0:
                    st.success(f"Đã nhập cho {restocked} mã hàng.")
                    time.sleep(1.5)
                    st.rerun()
                elif restocked == 0:
                    st.info("Kho vẫn đủ hàng!")

        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("🚪 Đăng Xuất"):
            logout()

    else:
        st.markdown(f"### Hồ Sơ: {u['name']}")
        st.write(f"📍 Địa chỉ: {u.get('location', '')}")
        st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

        # ── Review panel ──
        st.markdown("#### ⭐ Viết đánh giá sản phẩm")
        with st.form("review_form"):
            review_options = {p["id"]: p["name"] for p in PRODUCTS}
            review_pid  = st.selectbox("Chọn sản phẩm:", list(review_options.keys()),
                                        format_func=lambda x: review_options[x])
            review_rating  = st.slider("Số sao:", 1, 5, 5)
            review_comment = st.text_area("Nhận xét:", placeholder="Chia sẻ trải nghiệm của bạn...")
            if st.form_submit_button("Gửi đánh giá"):
                if submit_review(review_pid, review_rating, review_comment):
                    st.success("✅ Cảm ơn bạn đã đánh giá!")
                else:
                    st.error("Không thể gửi đánh giá. Vui lòng thử lại.")

        st.markdown('<div class="divider" style="margin-top: 2rem;"></div>', unsafe_allow_html=True)
        if st.button("🚪 Đăng Xuất"):
            logout()

# ══════════════════════════════════════════════════════════════════
# MAIN ROUTING
# ══════════════════════════════════════════════════════════════════
render_navbar()
render_notification()

tab_labels = ["🏠 Trang Chủ", "🛍️ Sản Phẩm", "🛒 Giỏ Hàng", "👤 Tài Khoản"]
tab_map    = ["home", "products", "cart", "login" if not st.session_state.logged_in else "profile"]

cols = st.columns(len(tab_labels))
for i, (label, key) in enumerate(zip(tab_labels, tab_map)):
    with cols[i]:
        if st.button(label, key=f"nav_{i}", use_container_width=True):
            navigate(key)

st.markdown("<hr>", unsafe_allow_html=True)

page = st.session_state.page
if   page == "home":     page_home()
elif page == "products": page_products()
elif page == "cart":     page_cart()
elif page == "login":    page_login()
elif page == "profile":  page_profile()