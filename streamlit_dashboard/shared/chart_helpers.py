import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Plotly theme config
PLOTLY_THEME = dict(
    template      = "plotly_dark",
    paper_bgcolor = "rgba(0,0,0,0)",
    plot_bgcolor  = "rgba(0,0,0,0)",
    font_color    = "#9ca3af",
    margin        = dict(l=0, r=0, t=28, b=0),
    height        = 500,
)


def fig_line_dual(df, x, y1, y2, y1_name, y2_name, title=""):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df[x], y=df[y1], name=y1_name,
                             line=dict(color="#5b8dee", width=2), mode="lines+markers"))
    fig.add_trace(go.Scatter(x=df[x], y=df[y2], name=y2_name, yaxis="y2",
                             line=dict(color="#3ecf8e", width=1.5, dash="dot"),
                             mode="lines+markers"))
    fig.update_layout(
        title=title,
        yaxis=dict(title=y1_name, tickprefix="$"),
        yaxis2=dict(title=y2_name, overlaying="y", side="right"),
        legend=dict(orientation="h", y=1.08),
        **PLOTLY_THEME,
    )
    return fig


def fig_hbar(df, x, y, color=None, title=""):
    fig = px.bar(df, x=x, y=y, orientation="h", color=color,
                 color_discrete_sequence=["#5b8dee","#3ecf8e","#ab47bc",
                                          "#f9a825","#f06292","#26c6da"],
                 title=title)
    fig.update_layout(yaxis_categoryorder="total ascending",
                      showlegend=False, **PLOTLY_THEME)
    return fig


def fig_donut(df, names, values, title=""):
    fig = px.pie(df, names=names, values=values, hole=0.65,
                 color_discrete_sequence=["#5b8dee","#3ecf8e","#ab47bc","#f9a825",
                                          "#f06292","#ef5350"],
                 title=title)
    fig.update_layout(legend=dict(orientation="h", y=-0.1), **PLOTLY_THEME)
    return fig


def fig_stacked_bar(df, x, y, color, title=""):
    color_map = {
        "Đã giao": "#3ecf8e",
        "Đang giao": "#5b8dee",
        "Đang xử lý": "#ab47bc",
        "Đang chờ": "#f9a825",
        "Đã hủy": "#ef5350",
        "Đã hoàn": "#ef5350",
    }
    fig = px.bar(df, x=x, y=y, color=color, title=title,
                 color_discrete_map=color_map)
    fig.update_layout(barmode="stack", legend=dict(orientation="h", y=1.1),
                      **PLOTLY_THEME)
    return fig


def fig_live_orders(df):
    if df.empty:
        return go.Figure()
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df["minute"], y=df["new_orders"],
                             fill="tozeroy", name="New orders",
                             line=dict(color="#3ecf8e", width=1.5)))
    fig.add_trace(go.Scatter(x=df["minute"], y=df["cancellations"],
                             fill="tozeroy", name="Cancellations",
                             line=dict(color="#ef5350", width=1)))
    fig.update_layout(legend=dict(orientation="h", y=1.1), **PLOTLY_THEME)
    return fig

# def fig_stock_direction(df, title=""):
#     fig = go.Figure()
#     fig.add_trace(go.Bar(
#         x=df["time_period"], y=df["stock_in"],
#         name="Stock IN", marker_color="#3ecf8e"
#     ))
#     # Dữ liệu xuất kho là số âm nên thanh bar sẽ tự động chĩa xuống dưới
#     fig.add_trace(go.Bar(
#         x=df["time_period"], y=df["stock_out"],
#         name="Stock OUT", marker_color="#ef5350"
#     ))
#     fig.update_layout(
#         title=title,
#         barmode="relative", # Quan trọng: Giúp bar âm/dương đối xứng nhau
#         legend=dict(orientation="h", y=1.12),
#         # height=550,
#         **PLOTLY_THEME,
#     )
#     return fig
def fig_stock_direction(df, title=""):
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["time_period"], y=df["stock_in"],
        name="Stock IN", marker_color="#3ecf8e"
    ))
    fig.add_trace(go.Bar(
        x=df["time_period"], y=df["stock_out"],
        name="Stock OUT", marker_color="#ef5350"
    ))
    
    # CHIẾU CODE VÀO ĐÂY NÈ
    fig.update_layout(
        title=title,
        barmode="relative", 
        legend=dict(orientation="h", y=1.12),
        
        # 1. Ép trục X thành dạng Category để các cột to ra
        xaxis=dict(
            type='category', 
            tickangle=-45 # Xoay nhãn thời gian nghiêng xíu cho dễ đọc
        ),
        
        # 2. Điều chỉnh khoảng cách giữa các cột (0.1 = 10%)
        bargap=0.1, 
        
        **PLOTLY_THEME,
    )
    return fig