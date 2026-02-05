from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from metrics import CloudSQLMetrics
from utils import bytes_to_unit, get_disk_iops_tp



def _safe_xy(ts) -> Tuple[List[datetime], List[float]]:
    """Return (x, y) sorted by time from a TimeSeries-like object."""
    try:
        vals = list(ts.values)  # List[(datetime, value)]
        if not vals:
            return [], []
        vals.sort(key=lambda x: x[0])
        x = [t for t, _ in vals]
        y = [float(v) for _, v in vals]
        return x, y
    except Exception:
        return [], []

def disk_overview(metrics: CloudSQLMetrics) -> go.Figure:
    # --- Make Fig ---

    fig = make_subplots(
        rows=3,
        cols=1,
        specs=[
            [{"type": "xy", "secondary_y": True}],
            [{"type": "xy"}],
            [{"type": "xy"}],
        ],
        row_heights=[0.3, 0.35, 0.35],
        column_widths=[1],
        horizontal_spacing=0.08,
        vertical_spacing=0.04,
        subplot_titles=[
            "Disk Usage",
            "Disk Read/Write Ops Count",
            "Disk Throughput"
        ]
    )

    # --- Figure I: Disk Space Usage---
    x_ts = metrics.disk_utilization.timestamps()

    for d_type, values in metrics.disk_bytes_used_by_type.items():
        fig.add_trace(
            go.Scatter(
                x=values.timestamps(),
                y=[bytes_to_unit(v) for v in values.data()],
                name=d_type,
                mode="lines",
                # line=dict(color=CONNECTION_STATE_COLORS[state]),
                stackgroup="one",
                hovertemplate=(
                    "<b>%{y:.2f} GiB</b>"
                ),
                showlegend=False,
                visible=True,
            ),
            row=1, col=1,
        )

    fig.add_trace(
        go.Scatter(
            x=x_ts,
            y=[bytes_to_unit(v) for v in metrics.disk_quota.data()],
            mode="lines",
            line=dict(
                color="lightcoral",
                dash="dash",
                width=2
            ),
            hovertemplate=(
                "<b>Time:</b> %{x|%H:%M} - "
                "%{x|%Y/%m/%d} - "
                "%{x|%a}<br>"
                "<b>%{y:.2f} GiB</b><extra></extra>"
            )
            ,
            showlegend=False,
        ),
        secondary_y=False,
        row=1, col=1
    )

    fig.add_annotation(
        x=x_ts[-15],
        y=bytes_to_unit(metrics.disk_quota.data()[-1]),
        text="Quota",
        showarrow=False,
        font=dict(
            color="white",
            size=12,
        ),
        bgcolor="lightcoral",  # 填滿背景
        bordercolor="lightcoral",  # 邊框顏色
        borderwidth=0.5,  # 邊框粗細
        borderpad=1,  # 文字與框的內距
        xanchor="left",
        yanchor="bottom",
        row=1,col=1,
    )

    # --- Figure II: Disk IOPs---


    # --- Formatting ---
    fig.update_xaxes(
        showticklabels=False,
        ticks="",
        row=1, col=1,
    )
    fig.update_xaxes(
        showticklabels=False,
        ticks="",
        row=2, col=1,
    )
    fig.update_xaxes(
        tickformat="%H:%M<br>%Y/%m/%d<br>%a",
        row=3, col=1
    )


    # fig.update_yaxes(
    #     title_text="CPU Usage Time(CPU-seconds)",
    #     title_font=dict(color="lightblue"),
    #     # tickfont=dict(color="grey"),
    #     secondary_y=True,
    #     row=1, col=1
    # )

    fig.update_yaxes(
        title_text="GiB",
        row=1, col=1
    )

    fig.update_yaxes(
        title_text="GiB",
        row=3, col=1
    )


    fig.update_layout(
        hovermode="x",  # <- one hover box containing ALL traces at that x
        hoverdistance=-1,
        # hoverdistance=50,  # optional: how far from the cursor Plotly will look for points
    )

    fig.update_layout(
        height=800,
        margin=dict(l=20, r=20, t=60, b=120),
        legend=dict(
            orientation="h",
            xanchor="left",
            x=0.0,
            yanchor="top",
            y=-0.10
        ))

    return fig

def disk_ops(metrics: CloudSQLMetrics) -> go.Figure:
    fig = go.Figure()

    tier = metrics.instance_details["tier"]
    availability = metrics.instance_details["availability"]
    disk_iops_tp = get_disk_iops_tp(tier, availability)
    read_iops, write_iops = disk_iops_tp["max_iops_rw"]
    read_iops, write_iops = read_iops*60, write_iops*60
    x_ts = metrics.disk_write_ops.timestamps()



    fig.add_trace(
            go.Scatter(
                x=x_ts,
                y=metrics.disk_read_ops.data(),
                name="read_ops",
                mode="lines",
                line=dict(color="lightblue"),
                hovertemplate=(
                    "<b>%{y}</b>"
                ),
                showlegend=False,
                visible=True,
            ),
        )

    fig.add_trace(
            go.Scatter(
                x=x_ts,
                y=metrics.disk_write_ops.data(),
                name="write_ops",
                mode="lines",
                line=dict(color="lightcoral"),
                hovertemplate=(
                    "<b>%{y}</b>"
                ),
                showlegend=False,
                visible=True,
            ),
        )
    fig.add_trace(
        go.Scatter(
            x=x_ts,
            y=[read_iops] * len(x_ts),
            mode="lines",
            line=dict(
                color="blue",
                dash="dash",
                width=2
            ),
            hovertemplate=(
                "<b>%{y}</b><extra></extra>"
            )
            ,
            showlegend=False,
        ),
    )
    fig.add_trace(
        go.Scatter(
            x=x_ts,
            y=[write_iops] * len(x_ts),
            mode="lines",
            line=dict(
                color="red",
                dash="dash",
                width=2
            ),
            hovertemplate=(
                "<b>Time:</b> %{x|%H:%M} - "
                "%{x|%Y/%m/%d} - "
                "%{x|%a}<br>"
                "<b>%{y}</b><extra></extra>"
            )
            ,
            showlegend=False,
        ),
    )
    # --- formatting ---
    fig.add_annotation(
        x=x_ts[-30],
        y=read_iops,
        text="Read max",
        showarrow=False,
        font=dict(
            color="white",
            size=12,
        ),
        bgcolor="blue",  # 填滿背景
        bordercolor="blue",  # 邊框顏色
        borderwidth=0.5,  # 邊框粗細
        borderpad=1,  # 文字與框的內距
        xanchor="left",
        yanchor="bottom",
    )
    fig.add_annotation(
        x=x_ts[-15],
        y=write_iops,
        text="Write max",
        showarrow=False,
        font=dict(
            color="white",
            size=12,
        ),
        bgcolor="red",  # 填滿背景
        bordercolor="red",  # 邊框顏色
        borderwidth=0.5,  # 邊框粗細
        borderpad=1,  # 文字與框的內距
        xanchor="left",
        yanchor="bottom",
    )
    fig.update_xaxes(
        tickformat="%H:%M<br>%Y/%m/%d<br>%a",
    )
    fig.update_yaxes(
        title_text="Count",
    )

    fig.update_layout(
        hovermode="x",  # <- one hover box containing ALL traces at that x
        hoverdistance=-1,
        height=300,
    )
    return fig

def disk_usage_pie_overview(
    metrics: CloudSQLMetrics,
    title: str = "Cloud SQL Disk Usage (Current + Trend)",
) -> go.Figure:
    """
    Two subfigures SIDE-BY-SIDE:

    Left:
      - Donut pie showing ALL used types + optional remainder + Available
      - NO legend

    Right:
      - Disk used over time (total + by-type)
      - Legend shown on the RIGHT side
      - Toggleable 90% quota warning line
    """
    cur_quota = metrics.disk_quota.data()[-1]
    cur_used = metrics.disk_bytes_used.data()[-1]
    cur_avail_bytes = max(cur_quota - cur_used, 0.0)

    by_type_b: Dict[str, float] = {}
    for k, ts in metrics.disk_bytes_used_by_type.items():
        cur_used_k_bytes = ts.data()[-1]
        if cur_used_k_bytes is not None and cur_used_k_bytes >= 0:
            by_type_b[k] = cur_used_k_bytes

    labels: List[str] = []
    values: List[float] = []

    for item in sorted(by_type_b.items(), key=lambda x: x[1]):
        labels.append(item[0])
        values.append(bytes_to_unit(item[1]))

    labels.append("Available")
    values.append(bytes_to_unit(cur_avail_bytes))


    fig = make_subplots(
        rows=1,
        cols=2,
        specs=[[{"type": "domain"}, {"type": "xy"}]],
        column_widths=[0.35, 0.65],
        horizontal_spacing=0.10,
        subplot_titles=(
            "Current disk usage",
            "Disk used over time",
        ),
    )

    fig.add_trace(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.35,
            sort=False,
            textinfo="label",
            textposition="inside",
            insidetextorientation="radial",
            hovertemplate="<b>%{label}</b><br>%{value:.2f} GiB<extra></extra>",
            showlegend=False,
        ),
        row=1,
        col=1,
    )

    # -------------------
    # RIGHT: Time series (legend ON)
    # -------------------
    x_used = metrics.disk_quota.timestamps()
    y_used = metrics.disk_quota.data()
    fig.add_trace(
        go.Scatter(
            x=x_used,
            y=[bytes_to_unit(v) for v in y_used],
            mode="lines",
            name="quota",
            hovertemplate="%{x}<br>%{y:.2f} GiB<extra></extra>",
        ),
        row=1,
        col=2,
    )




    x_used = metrics.disk_bytes_used.timestamps()
    y_used = metrics.disk_bytes_used.data()
    fig.add_trace(
        go.Scatter(
            x=x_used,
            y=[bytes_to_unit(v) for v in y_used],
            mode="lines",
            name="disk_bytes_used",
            hovertemplate="%{x}<br>%{y:.2f} GiB<extra></extra>",
        ),
        row=1,
        col=2,
    )

    for type_name, ts in metrics.disk_bytes_used_by_type.items():
        fig.add_trace(
            go.Scatter(
                x=ts.timestamps(),
                y=[bytes_to_unit(v) for v in ts.data()],
                mode="lines",
                name=f"Type: {type_name}",
                hovertemplate="%{x}<br>%{y:.2f} GiB<extra></extra>",
            ),
            row=1,
            col=2,
        )

    # -------------------
    # Warning line + toggle
    # -------------------
    warn_x = metrics.disk_quota.timestamps()
    warn_y = [bytes_to_unit(v* 0.9)  for v in metrics.disk_quota.data()]  # 90% in GiB

    fig.add_trace(
        go.Scatter(
            x=warn_x,
            y=warn_y,
            mode="lines",
            name="Safe line (90% quota)",
            line=dict(color="red", dash="dash"),
            visible=False,
            hovertemplate="%{x}<br>%{y:.2f} GiB (90% quota)<extra></extra>",
        ),
        row=1,
        col=2,
    )

    # -------------------
    # Button to toggle the safe line
    # -------------------
    safe_trace_index = len(fig.data) - 1  # last trace we just added

    fig.update_layout(
        updatemenus=[
            dict(
                type="buttons",
                direction="left",
                x=0.62,  # position (tweak as you like)
                y=1.15,
                buttons=[
                    dict(
                        label="Safe line",
                        method="restyle",
                        args=[{"visible": [True]}, [safe_trace_index]],
                    ),
                    dict(
                        label="Hide safe line",
                        method="restyle",
                        args=[{"visible": [False]}, [safe_trace_index]],
                    ),
                ],
            )
        ]
    )

    fig.update_layout(
        # title_text="Cloud SQL Disk Usage",
        height=650,
        margin=dict(l=20, r=20, t=70, b=20),
    )

    fig.update_yaxes(
        title_text="GiB",
        # ticks="outside",
        # tickformat=".2f",
        rangemode="tozero",
        row=1,
        col=2,
    )

    return fig

