import streamlit as st
from pages.components import render_product_card
from shared.cart_actions import PRODUCTS

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
