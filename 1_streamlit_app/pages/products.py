import streamlit as st
from shared.kafka_client import send_kafka_event, TOPIC_PAGE_VIEW
from pages.components import render_product_card
from shared.cart_actions import PRODUCTS,CATEGORIES

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