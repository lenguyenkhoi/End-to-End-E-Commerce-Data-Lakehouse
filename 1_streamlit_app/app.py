import streamlit as st
# ── Shared modules ─────────────────────────────────────────────────
from shared.style import CSS
from shared.db import get_all_products
from shared.state import init_state, navigate

# ── Page modules ───────────────────────────────────────────────────
from pages.components import render_navbar, render_notification
from pages.home import page_home
from pages.products import page_products
from pages.cart import page_cart
from pages.login import page_login
from pages.profile import page_profile
from shared.cart_actions import PRODUCTS, CATEGORIES

# ── Page config ────────────────────────────────────────────────────
st.set_page_config(
    page_title="KHOI Store",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Bootstrap ─────────────────────────────────────────────────────
CSS()
init_state()

# ── Layout ────────────────────────────────────────────────────────
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

# ── Routing ───────────────────────────────────────────────────────
page = st.session_state.page
if   page == "home":     page_home()
elif page == "products": page_products()
elif page == "cart":     page_cart()
elif page == "login":    page_login()
elif page == "profile":  page_profile()