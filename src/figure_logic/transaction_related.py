from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional, Tuple

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from metrics import CloudSQLMetrics, PSQLStatementsExecutedCountMetric


def transaction_ops(metrics: CloudSQLMetrics) -> go.Figure:
    fig = go.Figure()

    x_avg = []
    avg_sum: int = 0
    avg_count: int = 0

    first_trace = True
    for item in  metrics.psql_transaction_count:
        if sum(item.psql_transaction_count.data()) < 5:
            continue

        x_avg = item.psql_transaction_count.timestamps() if x_avg == [] else x_avg
        avg_sum += sum(item.psql_transaction_count.data())
        avg_count = len(item.psql_transaction_count.data()) if avg_count == 0 else avg_count

        if first_trace:
            hovertemplate = (
                "<b>Time:</b> %{x|%H:%M} - "
                "%{x|%Y/%m/%d} - "
                "%{x|%a}<br><br>"
                "<b>Counts:</b> %{y}<br>"
            )
            first_trace = False
        else:
            hovertemplate = (
                        "<b>Counts:</b> %{y}<br>"
                    )

        unique_id = item.database + "(" + item.transaction_type + ")"
        fig.add_trace(
            go.Scatter(
                x=item.psql_transaction_count.timestamps(),
                y=item.psql_transaction_count.data(),
                name=unique_id,
                mode="lines",
                # line=dict(color=CONNECTION_STATE_COLORS[item.state]),
                stackgroup="one",
                legendgroup=unique_id,
                hovertemplate=hovertemplate,
                showlegend=True,
                visible=True,
            ),
        )

    avg_values = [round(avg_sum / avg_count)] * len(x_avg)

    fig.add_trace(
        go.Scatter(
            x=x_avg,
            y=avg_values,
            name="AVG Count",
            mode="lines",
            line=dict(
                color="lightcoral",
                dash="dash",
                width=2
            ),
            hovertemplate="<b>Avg. Counts:</b>  %{y}<br>",
            showlegend=True,
        )
    )
    fig.add_annotation(
        x=x_avg[-15],
        y=avg_values[-1],
        text="Avg. Count",
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
    )

    fig.update_xaxes(
        tickformat="%H:%M<br>%Y/%m/%d<br>%a",
    )

    fig.update_yaxes(
        title_text="Counts",
    )
    fig.update_layout(
        hovermode="x",  # <- one hover box containing ALL traces at that x
        hoverdistance=-1,
        # hoverdistance=50,  # optional: how far from the cursor Plotly will look for points
    )
    fig.update_layout(
        height=400,
        margin=dict(l=20, r=20, t=60, b=150),
        legend=dict(
            orientation="h",
            xanchor="left",
            x=0.0,
            yanchor="top",
            y=-0.25
        ))
    return fig


def statements_executed_count(metrics: CloudSQLMetrics) -> go.Figure:
    fig = go.Figure()

    first_trace = True
    for item in  metrics.psql_statements_executed_count_metrics:
        if sum(item.psql_statements_executed_count.data()) < 5:
            continue

        if first_trace:
            hovertemplate = (
                "<b>Time:</b> %{x|%H:%M} - "
                "%{x|%Y/%m/%d} - "
                "%{x|%a}<br><br>"
                "<b>Counts:</b> %{y}<br>"
            )
            first_trace = False
        else:
            hovertemplate = (
                        "<b>Counts:</b> %{y}<br>"
                    )

        unique_id = item.database + "(" + item.operation_type + ")"
        fig.add_trace(
            go.Scatter(
                x=item.psql_statements_executed_count.timestamps(),
                y=item.psql_statements_executed_count.data(),
                name=unique_id,
                mode="lines",
                # line=dict(color=CONNECTION_STATE_COLORS[item.state]),
                stackgroup="one",
                legendgroup=unique_id,
                hovertemplate=hovertemplate,
                showlegend=True,
                visible=True,
            ),
        )

        fig.update_xaxes(
            tickformat="%H:%M<br>%Y/%m/%d<br>%a",
        )

        fig.update_yaxes(
            title_text="Counts",
        )
        fig.update_layout(
            hovermode="x",  # <- one hover box containing ALL traces at that x
            hoverdistance=-1,
            # hoverdistance=50,  # optional: how far from the cursor Plotly will look for points
        )
        fig.update_layout(
            height=400,
            margin=dict(l=20, r=20, t=60, b=150),
            legend=dict(
                orientation="h",
                xanchor="left",
                x=0.0,
                yanchor="top",
                y=-0.25
            ))
    return fig