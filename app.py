import os

import pandas as pd
import plotly.graph_objects as go
import psycopg2
import streamlit as st

DSN = os.getenv("DB_DSN", "postgresql://stocks:stocks@localhost:5432/stocks")

st.set_page_config(page_title="Live Stock Pipeline", layout="wide")
st.title("Live Stock Prices")


def query(sql, params=None):
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [c[0] for c in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    finally:
        conn.close()


with st.sidebar:
    minutes = st.slider("Time window (minutes)", 5, 240, 30)
    try:
        symbols = query("SELECT DISTINCT symbol FROM stock_prices ORDER BY 1")["symbol"].tolist()
    except Exception:
        symbols = []
    selected = st.multiselect("Symbols", symbols, default=symbols)


@st.fragment(run_every=5)
def live():
    if not selected:
        st.info("Waiting for data... make sure the producer and consumer are running.")
        return

    df = query(
        """SELECT ts, symbol, price, pct_change, moving_avg
           FROM stock_prices
           WHERE symbol = ANY(%s) AND ts > now() - (%s || ' minutes')::interval
           ORDER BY ts""",
        (selected, str(minutes)),
    )
    if df.empty:
        st.info("No data in this window yet.")
        return

    latest = df.sort_values("ts").groupby("symbol").tail(1).set_index("symbol")
    cols = st.columns(len(latest))
    for col, (sym, row) in zip(cols, latest.iterrows()):
        col.metric(sym, f"{row.price:,.2f}", f"{row.pct_change:+.2f}%")

    sym = st.selectbox("Chart symbol", selected, key="chart_symbol")
    d = df[df.symbol == sym]
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=d.ts, y=d.price, name="Price", mode="lines"))
    fig.add_trace(go.Scatter(x=d.ts, y=d.moving_avg, name="Moving avg", mode="lines",
                             line=dict(dash="dash")))
    fig.update_layout(height=420, margin=dict(l=0, r=0, t=30, b=0), hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Recent alerts")
    alerts = query("SELECT ts, symbol, price, pct_change, message FROM alerts ORDER BY ts DESC LIMIT 10")
    if alerts.empty:
        st.caption("No alerts yet.")
    else:
        st.dataframe(alerts, use_container_width=True, hide_index=True)


live()
