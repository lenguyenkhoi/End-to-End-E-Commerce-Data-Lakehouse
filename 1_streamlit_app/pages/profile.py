import time
import streamlit as st
from shared.auth import logout
from shared.state import navigate
from shared.inventory import restock_item, auto_restock, submit_review
from shared.simulator import simulate_traffic, simulate_realtime_traffic
from shared.cart_actions import PRODUCTS, CATEGORIES

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
