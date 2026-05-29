import streamlit as st
from shared.auth import login, register
from shared.state import navigate
from shared.kafka_client import send_kafka_event, TOPIC_USER

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