import os
import base64
import streamlit as st

from shared.state import cart_count
from shared.kafka_client import send_kafka_event, TOPIC_PAGE_VIEW, TOPIC_CART
from shared.cart_actions import add_to_cart


# def render_navbar():
#     badge = f'<span class="cart-badge">{cart_count()}</span>' if cart_count() > 0 else ""
#     user_label = st.session_state.user["name"].split()[0] if st.session_state.logged_in else ""
#     html = (
#         f'<div class="navbar">'
#         f'<div class="navbar-brand">✦ KHOI STORE</div>'
#         f'<div class="navbar-links"><span class="nav-link">Sản phẩm mới</span><span class="nav-link">Danh mục</span></div>'
#         f'<div style="display:flex;align-items:center;gap:1rem;">'
#     )
#     if user_label:
#         html += f'<span style="color:var(--accent);font-weight:500;">👤 {user_label}</span>'
#     html += f'<span style="color:var(--cream);">🛒 Giỏ hàng {badge}</span></div></div>'
#     st.markdown(html, unsafe_allow_html=True)

# def render_notification():
#     if st.session_state.notification:
#         st.markdown(f'<div class="toast">{st.session_state.notification}</div>', unsafe_allow_html=True)
#         st.session_state.notification = None

# def render_product_card(p):
#     tag_html = (
#         f'<div style="position:relative;"><span style="position:absolute;top:-8px;right:0;'
#         f'background:var(--accent);color:white;font-size:0.65rem;padding:2px 8px;border-radius:20px;'
#         f'font-weight:600;z-index:10;">{p["tag"]}</span></div>'
#     ) if p.get("tag") else ""

#     current_dir  = os.path.dirname(os.path.abspath(__file__))
#     img_filename = p.get("image_url") or "null.jpg"
#     img_path     = os.path.join(current_dir, "..", "image", img_filename)

#     img_b64 = ""
#     if os.path.exists(img_path):
#         try:
#             with open(img_path, "rb") as f:
#                 img_b64 = base64.b64encode(f.read()).decode()
#         except:
#             pass

#     ext = "jpeg" if img_filename.lower().endswith("jpg") else "png"
#     if img_b64:
#         img_html = (
#             f'<div class="product-img" style="background-image: url(\'data:image/{ext};base64,{img_b64}\');'
#             f'background-size: contain; background-position: center; background-repeat: no-repeat;'
#             f'background-color: white; height: 200px; width: 100%; border-radius: 16px 16px 0 0;"></div>'
#         )
#     else:
#         img_html = (
#             f'<div class="product-img" style="height: 200px; background-color: var(--cream); color: #dc3545;'
#             f'display:flex; align-items:center; justify-content:center; text-align:center;'
#             f'border-radius: 16px 16px 0 0;">📷 Thiếu file:<br><b>{img_filename}</b></div>'
#         )

#     html_content = (
#         f'<div class="product-card" style="padding-bottom: 0px; border-bottom: none;'
#         f'border-bottom-left-radius: 0; border-bottom-right-radius: 0; height: 100%;">'
#         f'{tag_html}{img_html}'
#         f'<div class="product-body" style="height: 120px;">'
#         f'<div style="display:flex; justify-content:space-between; margin-bottom: 5px;">'
#         f'<div style="color:var(--accent);font-size:0.7rem;text-transform:uppercase;font-weight:bold;">'
#         f'{p["brand"]} | {p["category"]}</div>'
#         f'<div style="color:var(--muted);font-size:0.7rem;font-weight:bold;">Kho: {p.get("stock", 0)}</div>'
#         f'</div>'
#         f'<div class="product-name" style="font-size: 1rem; overflow: hidden; text-overflow: ellipsis;'
#         f'display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;">{p["name"]}</div>'
#         f'<div class="product-price" style="margin-top: 8px;">{p["price"]:,} đ</div>'
#         f'</div></div>'
#     )
#     st.markdown(html_content, unsafe_allow_html=True)
    
#     col1, col2 = st.columns(2)
#     with col1:
#         if st.button("Xem chi tiết sản phẩm", key=f"view_{p['id']}", use_container_width=True):
#             # Bắn sự kiện xem sản phẩm lên Kafka
#             send_kafka_event(TOPIC_PAGE_VIEW, "product_viewed", {
#                 "page_name": "products",
#                 "product_id": p["id"], "product_name": p["name"],
#                 "category": p["category"], "brand": p["brand"], "price": p["price"],
#                 "trigger": "click_view_button",
#             })
#             st.session_state.notification = f"Bạn đang xem thông tin {p['name']}!"
#             st.rerun()
#     with col2:
#         if st.button("🛒 Thêm", key=f"atc_{p['id']}", use_container_width=True):
#             add_to_cart(p["id"])
#             # Vừa thêm vào giỏ vừa ghi nhận là đã xem
#             send_kafka_event(TOPIC_PAGE_VIEW, "product_viewed", {
#                 "page_name": "products",
#                 "product_id": p["id"], "product_name": p["name"],
#                 "category": p["category"], "brand": p["brand"], "price": p["price"],
#                 "trigger": "add_to_cart",
#             })
#             st.rerun()



 
def render_navbar():
    badge      = f'<span class="cart-badge">{cart_count()}</span>' if cart_count() > 0 else ""
    user_label = st.session_state.user["name"].split()[0] if st.session_state.logged_in else ""
    html = (
        f'<div class="navbar">'
        f'<div class="navbar-brand">✦ KHOI STORE</div>'
        f'<div class="navbar-links"><span class="nav-link">Sản phẩm mới</span><span class="nav-link">Danh mục</span></div>'
        f'<div style="display:flex;align-items:center;gap:1rem;">'
    )
    if user_label:
        html += f'<span style="color:var(--accent);font-weight:500;">👤 {user_label}</span>'
    html += f'<span style="color:var(--cream);">🛒 Giỏ hàng {badge}</span></div></div>'
    st.markdown(html, unsafe_allow_html=True)
 
 
def render_notification():
    if st.session_state.notification:
        st.markdown(f'<div class="toast">{st.session_state.notification}</div>', unsafe_allow_html=True)
        st.session_state.notification = None
 
 
def _get_image_mime(filename: str) -> str:
    """Return the correct MIME sub-type for a given image filename."""
    ext = os.path.splitext(filename)[1].lower()
    return {
        ".jpg":  "jpeg",
        ".jpeg": "jpeg",
        ".png":  "png",
        ".webp": "webp",
        ".gif":  "gif",
    }.get(ext, "jpeg")   # safe default
 
 
def _load_image_b64(img_filename: str) -> str:
    """
    Try several candidate paths to find the image file.
    Returns base64-encoded string, or empty string if not found.
 
    Candidate order:
      1. <project_root>/image/<filename>   — standard layout
      2. <pages_dir>/../image/<filename>   — relative to this file
      3. ./image/<filename>                — cwd (where `streamlit run` is called)
    """
    pages_dir    = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(pages_dir)           # one level up from pages/
    cwd          = os.getcwd()
 
    candidates = [
        os.path.join(project_root, "image", img_filename),
        os.path.join(pages_dir, "..", "image", img_filename),
        os.path.join(cwd, "image", img_filename),
    ]
 
    for path in candidates:
        norm = os.path.normpath(path)
        if os.path.isfile(norm):
            try:
                with open(norm, "rb") as f:
                    return base64.b64encode(f.read()).decode()
            except Exception:
                continue   # try next candidate
 
    return ""   # not found anywhere
 
 
def render_product_card(p):
    tag_html = (
        f'<div style="position:relative;"><span style="position:absolute;top:-8px;right:0;'
        f'background:var(--accent);color:white;font-size:0.65rem;padding:2px 8px;border-radius:20px;'
        f'font-weight:600;z-index:10;">{p["tag"]}</span></div>'
    ) if p.get("tag") else ""
 
    img_filename = p.get("image_url") or "null.jpg"
    img_b64      = _load_image_b64(img_filename)
    mime         = _get_image_mime(img_filename)
 
    if img_b64:
        img_html = (
            f'<div class="product-img" style="background-image: url(\'data:image/{mime};base64,{img_b64}\');'
            f'background-size: contain; background-position: center; background-repeat: no-repeat;'
            f'background-color: white; height: 200px; width: 100%; border-radius: 16px 16px 0 0;"></div>'
        )
    else:
        img_html = (
            f'<div class="product-img" style="height: 200px; background-color: var(--cream); color: #dc3545;'
            f'display:flex; align-items:center; justify-content:center; text-align:center;'
            f'border-radius: 16px 16px 0 0;">📷 Thiếu file:<br><b>{img_filename}</b></div>'
        )
 
    html_content = (
        f'<div class="product-card" style="padding-bottom: 0px; border-bottom: none;'
        f'border-bottom-left-radius: 0; border-bottom-right-radius: 0; height: 100%;">'
        f'{tag_html}{img_html}'
        f'<div class="product-body" style="height: 120px;">'
        f'<div style="display:flex; justify-content:space-between; margin-bottom: 5px;">'
        f'<div style="color:var(--accent);font-size:0.7rem;text-transform:uppercase;font-weight:bold;">'
        f'{p["brand"]} | {p["category"]}</div>'
        f'<div style="color:var(--muted);font-size:0.7rem;font-weight:bold;">Kho: {p.get("stock", 0)}</div>'
        f'</div>'
        f'<div class="product-name" style="font-size: 1rem; overflow: hidden; text-overflow: ellipsis;'
        f'display: -webkit-box; -webkit-line-clamp: 2; -webkit-box-orient: vertical;">{p["name"]}</div>'
        f'<div class="product-price" style="margin-top: 8px;">{p["price"]:,} đ</div>'
        f'</div></div>'
    )
    st.markdown(html_content, unsafe_allow_html=True)
 
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Xem chi tiết sản phẩm", key=f"view_{p['id']}", use_container_width=True):
            send_kafka_event(TOPIC_PAGE_VIEW, "product_viewed", {
                "page_name": "products",
                "product_id": p["id"], "product_name": p["name"],
                "category": p["category"], "brand": p["brand"], "price": p["price"],
                "trigger": "click_view_button",
            })
            st.session_state.notification = f"Bạn đang xem thông tin {p['name']}!"
            st.rerun()
    with col2:
        if st.button("🛒 Thêm", key=f"atc_{p['id']}", use_container_width=True):
            add_to_cart(p["id"])
            send_kafka_event(TOPIC_PAGE_VIEW, "product_viewed", {
                "page_name": "products",
                "product_id": p["id"], "product_name": p["name"],
                "category": p["category"], "brand": p["brand"], "price": p["price"],
                "trigger": "add_to_cart",
            })
            st.rerun()