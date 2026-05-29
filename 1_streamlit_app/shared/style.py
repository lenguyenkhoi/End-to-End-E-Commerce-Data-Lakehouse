import streamlit as st 

def CSS():
    CSS =st.markdown("""
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Playfair+Display:wght@400;600;700&family=DM+Sans:wght@300;400;500&display=swap');
        :root { --cream: #F7F3EE; --dark: #1A1714; --accent: #C8956C; --muted: #8A7F78; --card-bg: #FFFFFF; --border: #E8E0D8; }
        html, body, [data-testid="stAppViewContainer"] { background-color: var(--cream) !important; font-family: 'DM Sans', sans-serif; color: var(--dark); }
        [data-testid="stHeader"] { background: transparent !important; }
        [data-testid="stSidebar"] { background: var(--dark) !important; }
        [data-testid="stSidebar"] * { color: var(--cream) !important; }
        h1, h2, h3 { font-family: 'Playfair Display', serif !important; }
        #MainMenu, footer, header { visibility: hidden; }
        .block-container { padding-top: 1rem !important; }
        .navbar { display: flex; justify-content: space-between; align-items: center; padding: 1rem 2rem; background: var(--dark); border-radius: 16px; margin-bottom: 2rem; color: var(--cream); }
        .navbar-brand { font-family: 'Playfair Display', serif; font-size: 1.8rem; font-weight: 700; color: var(--accent) !important; letter-spacing: 2px; }
        .navbar-links { display: flex; gap: 2rem; align-items: center; }
        .nav-link { color: var(--cream) !important; font-size: 0.9rem; font-weight: 500; text-transform: uppercase; opacity: 0.8;}
        .cart-badge { background: var(--accent); color: white; border-radius: 50%; padding: 2px 8px; font-size: 0.75rem; font-weight: 700; margin-left: 4px; }
        .hero { background: linear-gradient(135deg, var(--dark) 0%, #3D2E25 100%); border-radius: 20px; padding: 4rem 3rem; margin-bottom: 3rem; text-align: center; }
        .hero-eyebrow { color: var(--accent); font-size: 0.8rem; letter-spacing: 4px; text-transform: uppercase; margin-bottom: 1rem; }
        .hero-title { font-family: 'Playfair Display', serif; font-size: 3.5rem; font-weight: 700; color: var(--cream); line-height: 1.2; margin-bottom: 1.2rem; }
        .hero-sub { color: rgba(247,243,238,0.7); font-size: 1.1rem; max-width: 500px; margin: 0 auto 2rem; }
        .section-heading { font-family: 'Playfair Display', serif; font-size: 2rem; font-weight: 600; margin-bottom: 0.25rem; }
        .section-sub { color: var(--muted); font-size: 0.9rem; margin-bottom: 2rem; text-transform: uppercase; }
        .divider { width: 50px; height: 3px; background: var(--accent); margin-bottom: 2rem; }
        .product-card { background: var(--card-bg); border-radius: 16px; border: 1px solid var(--border); transition: transform 0.25s, box-shadow 0.25s; height: 100%; padding-bottom: 1rem;}
        .product-card:hover { transform: translateY(-4px); box-shadow: 0 16px 40px rgba(26,23,20,0.12); }
        .product-img { width: 100%; height: 200px; display: flex; align-items: center; justify-content: center; font-size: 4rem; background: var(--cream); }
        .product-body { padding: 1.25rem; }
        .product-name { font-family: 'Playfair Display', serif; font-size: 1.1rem; font-weight: 600; margin-bottom: 0.4rem; }
        .product-price { font-size: 1.2rem; font-weight: 700; color: var(--dark); }
        .stButton > button { background: var(--dark) !important; color: var(--cream) !important; border-radius: 10px !important; font-weight: 500 !important; padding: 0.55rem 1.4rem !important; transition: background 0.2s !important; }
        .stButton > button:hover { background: var(--accent) !important; }
        .toast { background: var(--dark); color: var(--cream); border-left: 4px solid var(--accent); padding: 0.9rem 1.4rem; border-radius: 10px; margin-bottom: 1rem; }
        </style>
        """, unsafe_allow_html=True) 
    return CSS