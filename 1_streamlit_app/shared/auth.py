import uuid
import streamlit as st
from psycopg2.extras import RealDictCursor
from shared.db import init_connection
from shared.kafka_client import send_kafka_event, TOPIC_USER


def login(username, password):
    """_summary_ Xử lý đăng nhập, kiểm tra thông tin trong database, lưu lại sự kiện lên kafka

    Args:
        username (_type_): _description_ Tên đăng nhập
        password (_type_): _description_ Mật khẩu người dùng

    Returns:
        _type_: _description_ true nếu đăng nhập thành công, False nếu thất bại
    """
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
    """
    Xử lý đăng ký tài khoản mới, lưu thông tin vào database và lưu lại sự kiễn trên kafka
    Args:
        name (_type_): _description_ Họ và tên đầy đủ
        email (_type_): _description_ Địa chỉ email
        phone (_type_): _description_ Số điện thoại
        gender (_type_): _description_ Giới tính
        location (_type_): _description_ Địa điểm
        username (_type_): _description_ Tên đăng nhập
        password (_type_): _description_ Mật khẩu người dùng

    Returns:
        _type_: _description_ true nếu đăng ký thành công, False nếu thất bại
    """
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
    """_summary_ Xử lý đăng xuất người dùng.
    """
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
