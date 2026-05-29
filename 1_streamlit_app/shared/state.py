import uuid
import streamlit as st
from shared.kafka_client import send_kafka_event, TOPIC_PAGE_VIEW
from shared.cart_actions import PRODUCTS, CATEGORIES

def init_state():
    """_summary_ khởi tạo các biến cần thiết trong session_sate bao gồm:
    - logged_in: trạng thái đã đăng nhập 
    - user: thông tin user đã đăng nhập 
    - cart: giỏ hàng
    - page: trang hiện tại đang xem 
    - notification: thông báo hiển thị cho ngườ dùng 
    - session_id: mã định danh phiên, gửi lên kafka 
    - applied_promo: mã giảm giá đã áp dụng, lưu dưới dạng dict {"promo_id": ..., "promo_code": ..., "discount_percent": ...}
    """
    defaults = {
        "logged_in": False,
        "user": None,
        "cart": {},
        "page": "home",
        "notification": None,
        "session_id": str(uuid.uuid4()),
        "applied_promo": None,  # {"promo_id": ..., "promo_code": ..., "discount_percent": ...}
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def navigate(page):
    """_summary_ Cập nhật trang hiện tại trong session_state và gửi sự kiện page_viewed lên kafka 

    Args:
        page (_type_): _description_ Tên trang, ví dụ: home, products, cart, profile
    """
    st.session_state.page = page
    send_kafka_event(TOPIC_PAGE_VIEW, "page_viewed", {"page_name": page})
    st.rerun()


def cart_count():
    """_summary_ Tính tổng số lượng sản phẩm trong giỏ hàng

    Returns:
        int: Tổng số lượng sản phẩm
    """
    return sum(st.session_state.cart.values())


def cart_subtotal():
    """_summary_ 

    Args:
        products (_type_): _description_ Danh sách sản phẩm

    Returns:
        _type_: _description_ Tổng tiền chưa áp dụng 
    """
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
