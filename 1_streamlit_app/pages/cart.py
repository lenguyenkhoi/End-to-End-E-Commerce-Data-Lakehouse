import streamlit as st
from shared.state import cart_subtotal, apply_discount, navigate
from shared.cart_actions import PRODUCTS, CATEGORIES,remove_from_cart, update_cart_qty, apply_promo_code, remove_promo
from shared.inventory import checkout_order
from shared.cart_actions import PRODUCTS

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