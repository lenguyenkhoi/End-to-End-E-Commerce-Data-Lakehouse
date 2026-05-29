import uuid
import random
import string
import unicodedata
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
from shared.db import init_connection, get_all_products
from shared.cart_actions import PRODUCTS, CATEGORIES
from shared.kafka_client import (
    get_kafka_producer,
    TOPIC_USER, TOPIC_PAGE_VIEW, TOPIC_CART,
    TOPIC_PROMO, TOPIC_ORDER, TOPIC_INVENTORY, TOPIC_REVIEW,
)


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
            # TỰ ĐỘNG PHÂN TÍCH VÀ ĐIỀN PAGE_NAME CHO SIMULATOR
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
        # REAL-TIME LOGIC: Khách vào web lùi lại từ 0-3 phút so với hiện tại
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
            # Xem sản phẩm sau vài giây
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

        # Vào giỏ hàng sau vài chục giây
        cart_view_ts = (user_reg_dt + timedelta(seconds=random.randint(30, 90))).strftime('%Y-%m-%d %H:%M:%S')
        send_sim(TOPIC_PAGE_VIEW, "page_viewed", {
            "page_name": "cart",
            "trigger": "click_cart_icon"
        }, ts=cart_view_ts)

        # ── 4. CHỌN SẢN PHẨM ĐỂ MUA ─────────────────────────────
        num_items = random.randint(1, 3)
        chosen_products = random.sample(current_products, min(num_items, len(current_products)))

        # Đặt hàng diễn ra nhanh trong 1 đến 3 phút
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

                shipping_fee = random.choice([0, 15000, 30000])
                discount_amount = int(subtotal * promo["discount_percent"] / 100) if promo else 0
                total_amount = subtotal - discount_amount + shipping_fee
                payment = random.choice(PAYMENT_METHODS)

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
            "order_id": order_id,
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
            # Hoàn hàng sau 1 đến 5 phút
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