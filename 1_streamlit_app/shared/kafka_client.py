import os
import json
import uuid
import streamlit as st
from datetime import datetime
from kafka import KafkaProducer

# ── Kafka topics ───────────────────────────────────────────────────
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


@st.cache_resource
def get_kafka_producer():
    """_summary_ kafka config

    Returns:
        _type_: _description_ kafka producer
    """
    try:
        return KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_SERVER", "localhost:9092"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
        )
    except:
        return None


def _build_event(event_type: str, payload: dict, override_ts: str = None,
                 override_user: dict = None, override_session: str = None) -> dict:
    """Tạo envelope chuẩn cho mọi Kafka event."""
    user = override_user or (st.session_state.user if st.session_state.logged_in else None)
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
    """_summary_ Gửi event đến Kafka 

    Args:
        topic (str): _description_ Kafka topic
        event_type (str): _description_ Loại event, ví dụ: user_login, page_viewed
        payload (dict): _description_ Thông tin chi tiết của event
        override_ts (str, optional): _description_. Defaults to None. Nếu có, sẽ dùng timestamp này thay vì timestamp hiện tại
        override_user (dict, optional): _description_. Defaults to None. Nếu có, sẽ dùng thông tin user này thay vì user hiện tại trong session
        override_session (str, optional): _description_. Defaults to None. Nếu có, sẽ dùng session_id này thay vì session_id hiện tại trong session
    """
    producer = get_kafka_producer()
    if not producer:
        return
    if topic == TOPIC_PAGE_VIEW and "page_name" not in payload:
        payload["page_name"] = st.session_state.get("page", "unknown")
    event = _build_event(event_type, payload, override_ts, override_user, override_session)
    try:
        producer.send(topic, value=event)
        producer.flush(timeout=1.0)
    except:
        pass
