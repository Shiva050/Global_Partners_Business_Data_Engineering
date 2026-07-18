"""Global Partners — Business Insights dashboard.

Reads the gold tables from S3 and renders the six BRD dashboards. All heavy
computation lives in the PySpark gold jobs; this app only reads small,
pre-aggregated tables and draws them.

Run:  streamlit run dashboard/app.py     (needs AWS creds to read the bronze bucket)
"""
import io
import os

import boto3
import pandas as pd
import plotly.express as px
import pyarrow as pa
import pyarrow.parquet as pq
import streamlit as st

BUCKET = os.environ.get("GPB_BRONZE_BUCKET", "dms-global-partne-brusiness-bronze")
GOLD = f"s3://{BUCKET}/gold"

# --- validated light categorical palette (CVD worst-adjacent ΔE 11.0) ---
CAT = ["#0072B2", "#E69F00", "#009E73", "#D55E00", "#CC79A7"]
SEGMENT_COLORS = {"VIP": "#0072B2", "Regular": "#009E73", "New": "#E69F00", "Churn Risk": "#D55E00"}
TIER_COLORS = {"High": "#08519c", "Medium": "#4292c6", "Low": "#c6dbef"}      # sequential blue (ordinal)
STATUS_COLORS = {"Active": "#009E73", "At Risk": "#E69F00", "Dormant": "#D55E00"}  # reserved status
LOYALTY_COLORS = {"Loyalty": "#0072B2", "Non-Loyalty": "#E69F00"}

st.set_page_config(page_title="Global Partners — Business Insights", layout="wide")


@st.cache_data(ttl=600, show_spinner="Loading from S3…")
def load(name: str) -> pd.DataFrame:
    """List the table's parquet parts and read them (avoids s3fs HeadObject-on-prefix 403)."""
    s3 = boto3.client("s3")
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=f"gold/{name}/"):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
    tables = [pq.read_table(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=k)["Body"].read())) for k in keys]
    return pa.concat_tables(tables).to_pandas()


def style(fig, height=360):
    """Recessive axes/grid, tidy margins, unified hover — one house style."""
    fig.update_layout(
        height=height, margin=dict(l=10, r=10, t=40, b=10),
        plot_bgcolor="white", paper_bgcolor="white",
        font=dict(color="#1a1a19"), hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    fig.update_xaxes(showgrid=False, zeroline=False, linecolor="#d9dde1")
    fig.update_yaxes(showgrid=True, gridcolor="#eef1f4", zeroline=False)
    return fig


def kpi_row(items):
    for col, (label, value) in zip(st.columns(len(items)), items):
        col.metric(label, value)


# ---------------------------------------------------------------------------
def page_overview():
    st.header("Overview — Customer Lifetime Value")
    tiers = load("customer_clv_tiers")
    trends = load("clv_trends").sort_values("clv_date")

    kpi_row([
        ("Customers", f"{tiers['user_id'].nunique():,}"),
        ("Total CLV", f"${tiers['total_clv'].sum():,.0f}"),
        ("Avg CLV / customer", f"${tiers['total_clv'].mean():,.2f}"),
        ("Orders", f"{int(tiers['total_orders'].sum()):,}"),
    ])

    st.subheader("CLV evolving daily (portfolio)")
    st.caption("Cumulative lifetime value booked across all customers, and the average per acquired customer.")
    c1, c2 = st.columns([2, 1])
    fig = px.area(trends, x="clv_date", y="total_cumulative_clv",
                  labels={"clv_date": "", "total_cumulative_clv": "Total cumulative CLV ($)"})
    fig.update_traces(line_color=CAT[0], fillcolor="rgba(0,114,178,0.12)")
    c1.plotly_chart(style(fig), use_container_width=True)

    fig2 = px.line(trends, x="clv_date", y="avg_cumulative_clv",
                   labels={"clv_date": "", "avg_cumulative_clv": "Avg CLV / customer ($)"})
    fig2.update_traces(line_color=CAT[2])
    c2.plotly_chart(style(fig2), use_container_width=True)

    st.subheader("CLV value tiers")
    st.caption("Top 20% (High), middle 60% (Medium), bottom 20% (Low) by lifetime value.")
    t = tiers.groupby("clv_tier").agg(customers=("user_id", "nunique"),
                                      revenue=("total_clv", "sum")).reindex(["High", "Medium", "Low"]).reset_index()
    c3, c4 = st.columns(2)
    f3 = px.bar(t, x="clv_tier", y="customers", color="clv_tier", text="customers",
                color_discrete_map=TIER_COLORS, category_orders={"clv_tier": ["High", "Medium", "Low"]},
                labels={"clv_tier": "", "customers": "Customers"})
    f3.update_layout(showlegend=False)
    c3.plotly_chart(style(f3), use_container_width=True)
    f4 = px.bar(t, x="clv_tier", y="revenue", color="clv_tier", text_auto=".2s",
                color_discrete_map=TIER_COLORS, category_orders={"clv_tier": ["High", "Medium", "Low"]},
                labels={"clv_tier": "", "revenue": "Total CLV ($)"})
    f4.update_layout(showlegend=False)
    c4.plotly_chart(style(f4), use_container_width=True)
    with st.expander("Tier table"):
        st.dataframe(t, use_container_width=True)


def page_segmentation():
    st.header("Customer Segmentation (RFM)")
    rfm = load("customer_rfm")

    seg = rfm["rfm_segment"].value_counts().rename_axis("segment").reset_index(name="customers")
    kpi_row([("Customers", f"{len(rfm):,}"),
             ("VIPs", f"{int((rfm['rfm_segment'] == 'VIP').sum()):,}"),
             ("Churn Risk", f"{int((rfm['rfm_segment'] == 'Churn Risk').sum()):,}"),
             ("Avg RFM score", f"{rfm['rfm_score'].mean():.1f}")])

    c1, c2 = st.columns(2)
    f1 = px.bar(seg, x="segment", y="customers", color="segment", text="customers",
                color_discrete_map=SEGMENT_COLORS, labels={"segment": "", "customers": "Customers"})
    f1.update_layout(showlegend=False)
    c1.plotly_chart(style(f1), use_container_width=True)

    st.caption("Frequency vs monetary, coloured by segment (recency drives the segment too).")
    samp = rfm.sample(min(4000, len(rfm)), random_state=1)
    f2 = px.scatter(samp, x="frequency", y="monetary", color="rfm_segment",
                    color_discrete_map=SEGMENT_COLORS, opacity=0.6,
                    labels={"frequency": "Frequency (orders)", "monetary": "Monetary ($)", "rfm_segment": "Segment"})
    f2.update_yaxes(type="log")
    c2.plotly_chart(style(f2), use_container_width=True)

    with st.expander("Segment table"):
        st.dataframe(seg, use_container_width=True)


def page_churn():
    st.header("Churn Risk Indicators")
    churn = load("churn_indicators")

    counts = churn["churn_status"].value_counts().reindex(["Active", "At Risk", "Dormant"]).fillna(0)
    kpi_row([("Active", f"{int(counts['Active']):,}"),
             ("At Risk", f"{int(counts['At Risk']):,}"),
             ("Dormant", f"{int(counts['Dormant']):,}"),
             ("Median days since order", f"{churn['days_since_last_order'].median():.0f}")])

    c1, c2 = st.columns(2)
    cdf = counts.rename_axis("status").reset_index(name="customers")
    f1 = px.bar(cdf, x="status", y="customers", color="status", text="customers",
                color_discrete_map=STATUS_COLORS,
                category_orders={"status": ["Active", "At Risk", "Dormant"]},
                labels={"status": "", "customers": "Customers"})
    f1.update_layout(showlegend=False)
    c1.plotly_chart(style(f1), use_container_width=True)

    f2 = px.histogram(churn[churn["days_since_last_order"] <= 365], x="days_since_last_order", nbins=52,
                      labels={"days_since_last_order": "Days since last order"})
    f2.update_traces(marker_color=CAT[0])
    f2.add_vline(x=45, line_dash="dash", line_color="#D55E00", annotation_text="at-risk (45d)")
    c2.plotly_chart(style(f2), use_container_width=True)

    st.subheader("At-risk & dormant customers")
    at_risk = churn[churn["at_risk"]].sort_values("days_since_last_order", ascending=False)
    st.caption(f"{len(at_risk):,} customers past the 45-day threshold — candidates for re-engagement.")
    st.dataframe(at_risk[["user_id", "last_order_date", "days_since_last_order", "total_orders",
                          "avg_gap_days", "spend_change_pct", "churn_status"]].head(500),
                 use_container_width=True)


def page_sales():
    st.header("Sales Trends & Seasonality")
    sd = load("sales_daily")
    sd["order_date"] = pd.to_datetime(sd["order_date"])
    sd["month"] = sd["order_date"].dt.to_period("M").dt.to_timestamp()

    monthly = sd.groupby("month", as_index=False)["revenue"].sum()
    kpi_row([("Total revenue", f"${sd['revenue'].sum():,.0f}"),
             ("Categories", f"{sd['item_category'].nunique()}"),
             ("Locations", f"{sd['restaurant_id'].nunique()}"),
             ("Days covered", f"{sd['order_date'].nunique():,}")])

    st.subheader("Monthly revenue")
    f1 = px.area(monthly, x="month", y="revenue", labels={"month": "", "revenue": "Revenue ($)"})
    f1.update_traces(line_color=CAT[0], fillcolor="rgba(0,114,178,0.12)")
    st.plotly_chart(style(f1), use_container_width=True)

    c1, c2 = st.columns(2)
    cat = (sd.groupby("item_category", as_index=False)["revenue"].sum()
           .sort_values("revenue", ascending=False).head(10))
    f2 = px.bar(cat, x="revenue", y="item_category", orientation="h", text_auto=".2s",
                labels={"revenue": "Revenue ($)", "item_category": ""})
    f2.update_traces(marker_color=CAT[0])
    f2.update_yaxes(categoryorder="total ascending")
    c1.plotly_chart(style(f2), use_container_width=True)

    # Holiday effect: mean daily total revenue on holidays vs normal days
    daily = sd.groupby(["order_date", "is_holiday"], as_index=False)["revenue"].sum()
    hol = daily.groupby("is_holiday", as_index=False)["revenue"].mean()
    hol["kind"] = hol["is_holiday"].map({True: "Holiday", False: "Non-holiday"})
    f3 = px.bar(hol, x="kind", y="revenue", color="kind", text_auto=".3s",
                color_discrete_map={"Holiday": "#E69F00", "Non-holiday": "#0072B2"},
                labels={"kind": "", "revenue": "Avg daily revenue ($)"})
    f3.update_layout(showlegend=False)
    c2.plotly_chart(style(f3), use_container_width=True)


def page_loyalty():
    st.header("Loyalty Program Impact")
    loy = load("loyalty_impact").set_index("segment")
    order = ["Loyalty", "Non-Loyalty"]

    kpi_row([("Loyalty members", f"{int(loy.loc['Loyalty', 'customers']):,}"),
             ("Non-members", f"{int(loy.loc['Non-Loyalty', 'customers']):,}"),
             ("Loyalty repeat rate", f"{loy.loc['Loyalty', 'repeat_rate']:.0%}"),
             ("Non repeat rate", f"{loy.loc['Non-Loyalty', 'repeat_rate']:.0%}")])

    st.caption("Different scales, so shown as separate small charts (never a dual axis).")
    metrics = [("avg_clv", "Avg CLV ($)"), ("avg_order_value", "Avg order value ($)"),
               ("repeat_rate", "Repeat rate")]
    for col, (m, label) in zip(st.columns(3), metrics):
        d = loy.reindex(order).reset_index()
        f = px.bar(d, x="segment", y=m, color="segment", text_auto=".2f",
                   color_discrete_map=LOYALTY_COLORS, category_orders={"segment": order},
                   labels={"segment": "", m: label})
        f.update_layout(showlegend=False)
        col.plotly_chart(style(f, height=300), use_container_width=True)

    with st.expander("Loyalty table"):
        st.dataframe(loy.reset_index(), use_container_width=True)


def page_location():
    st.header("Location Performance")
    loc = load("location_performance").sort_values("revenue_rank")

    kpi_row([("Locations", f"{len(loc)}"),
             ("Top-store revenue", f"${loc['total_revenue'].max():,.0f}"),
             ("Median AOV", f"${loc['avg_order_value'].median():,.2f}"),
             ("Total revenue", f"${loc['total_revenue'].sum():,.0f}")])

    top = loc.head(15).copy()
    top["label"] = top["restaurant_id"].str.slice(0, 8)
    c1, c2 = st.columns(2)
    f1 = px.bar(top, x="total_revenue", y="label", orientation="h", text_auto=".2s",
                labels={"total_revenue": "Revenue ($)", "label": ""})
    f1.update_traces(marker_color=CAT[0])
    f1.update_yaxes(categoryorder="total ascending")
    c1.plotly_chart(style(f1, height=460), use_container_width=True)

    f2 = px.scatter(loc, x="order_count", y="avg_order_value", size="total_revenue",
                    hover_data=["restaurant_id"],
                    labels={"order_count": "Orders", "avg_order_value": "Avg order value ($)"})
    f2.update_traces(marker_color=CAT[2])
    c2.plotly_chart(style(f2, height=460), use_container_width=True)

    with st.expander("All locations"):
        st.dataframe(loc, use_container_width=True)


def page_discount():
    st.header("Pricing & Discount Effectiveness")
    d = load("discount_effectiveness")
    discounted = d[d["is_discounted"]]["orders"].sum() if d["is_discounted"].any() else 0

    kpi_row([("Discounted orders", f"{int(discounted):,}"),
             ("Full-price orders", f"{int(d.loc[~d['is_discounted'], 'orders'].sum()):,}"),
             ("Total discount given", f"${-d['total_discount'].sum():,.2f}")])

    if discounted == 0:
        st.info("**No discounts are present in this dataset** — every `option_price` is ≥ 0, so no "
                "order carries a discount. The pipeline detects discounts via `option_price < 0`; there "
                "are simply none to compare. (See docs/data_quality.md.)")

    st.subheader("Gross vs net revenue by pricing type")
    m = d.melt(id_vars="segment", value_vars=["total_gross_revenue", "total_net_revenue"],
               var_name="measure", value_name="amount")
    m["measure"] = m["measure"].map({"total_gross_revenue": "Gross", "total_net_revenue": "Net"})
    f = px.bar(m, x="segment", y="amount", color="measure", barmode="group", text_auto=".2s",
               color_discrete_map={"Gross": "#0072B2", "Net": "#009E73"},
               labels={"segment": "", "amount": "Revenue ($)", "measure": ""})
    st.plotly_chart(style(f), use_container_width=True)
    with st.expander("Discount table"):
        st.dataframe(d, use_container_width=True)


PAGES = {
    "Overview — CLV": page_overview,
    "Customer Segmentation": page_segmentation,
    "Churn Risk": page_churn,
    "Sales Trends": page_sales,
    "Loyalty Impact": page_loyalty,
    "Location Performance": page_location,
    "Discount Effectiveness": page_discount,
}


def main():
    st.sidebar.title("Global Partners")
    st.sidebar.caption("Business Insights")
    choice = st.sidebar.radio("Dashboard", list(PAGES))
    st.sidebar.divider()
    st.sidebar.caption(f"Source: `{GOLD}`")
    if st.sidebar.button("Refresh data"):
        st.cache_data.clear()
    PAGES[choice]()


if __name__ == "__main__":
    main()
