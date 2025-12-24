"""
Streamlit dashboard for the Snowflake Postgres transactional demo.

Provides interactive KPI charts over recent orders and inventory.
"""

import streamlit as st
import pandas as pd
import altair as alt

from app.db import db_cursor

# Snowflake-inspired color palette (primary blues and accents)
SNOWFLAKE_PALETTE = [
    "#29B5E8",  # Snowflake blue
    "#0071CE",  # deep blue
    "#00A3E0",  # bright cyan
    "#00CFFF",  # light cyan
    "#7FDBFF",  # soft blue
]


def get_summary_kpis(window_minutes: int, categories: list[str] | None = None):
    """
    Return high-level KPIs:
    - total_orders
    - total_revenue
    - orders_last_10_min
    - revenue_last_10_min
    """
    with db_cursor(dict_cursor=True) as cur:
        cur.execute(
            "SELECT * FROM fn_summary_kpis(%s, %s);",
            (window_minutes, categories if categories else None),
        )
        row = cur.fetchone() or {}

    total_orders = row.get("total_orders", 0) or 0
    total_revenue = float(row.get("total_revenue", 0) or 0)
    orders_window = row.get("orders_window", 0) or 0
    revenue_window = float(row.get("revenue_window", 0) or 0)
    active_customers_window = row.get("active_customers_window", 0) or 0
    avg_order_value_all = float(row.get("avg_order_value_all", 0) or 0)
    avg_order_value_window = float(row.get("avg_order_value_window", 0) or 0)
    orders_per_min_window = float(row.get("orders_per_min_window", 0) or 0)

    return {
        "total_orders": total_orders,
        "total_revenue": total_revenue,
        "orders_window": orders_window,
        "revenue_window": revenue_window,
        "active_customers_window": active_customers_window,
        "avg_order_value_all": avg_order_value_all,
        "avg_order_value_window": avg_order_value_window,
        "orders_per_min_window": orders_per_min_window,
    }


def get_recent_orders(
    limit: int = 20,
    window_minutes: int | None = None,
    categories: list[str] | None = None,
):
    """
    Return the most recent orders with customer and product details.
    """
    with db_cursor(dict_cursor=True) as cur:
        cur.execute(
            "SELECT * FROM fn_recent_orders(%s, %s, %s);",
            (window_minutes, limit, categories if categories else None),
        )
        return cur.fetchall()


def get_sales_by_category(
    window_minutes: int, categories: list[str] | None = None
):
    """
    Return aggregated sales by product category for the recent window,
    optionally filtered by category.
    """
    with db_cursor(dict_cursor=True) as cur:
        cur.execute(
            "SELECT * FROM fn_sales_by_category(%s, %s);",
            (window_minutes, categories if categories else None),
        )
        return cur.fetchall()


def get_inventory_snapshot(categories: list[str] | None = None):
    """
    Return current inventory levels per product.
    """
    with db_cursor(dict_cursor=True) as cur:
        cur.execute(
            "SELECT * FROM fn_inventory_snapshot(%s);",
            (categories if categories else None,),
        )
        return cur.fetchall()


def get_revenue_timeseries(
    window_minutes: int, categories: list[str] | None = None
):
    """
    Return revenue and order counts per minute over the recent window.
    """
    with db_cursor(dict_cursor=True) as cur:
        cur.execute(
            "SELECT * FROM fn_revenue_timeseries(%s, %s);",
            (window_minutes, categories if categories else None),
        )
        return cur.fetchall()


def get_top_customers(
    window_minutes: int, limit: int = 10, categories: list[str] | None = None
):
    """
    Return top customers by revenue in the recent window.
    """
    with db_cursor(dict_cursor=True) as cur:
        cur.execute(
            "SELECT * FROM fn_top_customers(%s, %s, %s);",
            (window_minutes, limit, categories if categories else None),
        )
        return cur.fetchall()


def main():
    st.set_page_config(
        page_title="Snowflake Postgres Live KPIs",
        layout="wide",
    )

    st.title("Snowflake Postgres – Live Transactional KPIs")
    st.caption(
        "Continuous ingestion every ~20 seconds. "
        "Use the controls to filter and refresh KPIs based on recent activity. "
        "Category filter applies to charts and 'last N minutes' KPIs."
    )

    # Sidebar filters / controls
    st.sidebar.header("Controls")
    window_minutes = st.sidebar.slider(
        "Time window (minutes)",
        min_value=5,
        max_value=120,
        value=30,
        step=5,
        help="Defines what counts as 'recent' for KPIs and sales by category.",
    )
    recent_limit = st.sidebar.slider(
        "Recent orders to show",
        min_value=10,
        max_value=200,
        value=50,
        step=10,
    )

    # Category filter for charts
    with db_cursor(dict_cursor=True) as cur:
        cur.execute("SELECT DISTINCT category FROM products ORDER BY category;")
        all_categories = [row["category"] for row in cur.fetchall()]

    selected_categories = st.sidebar.multiselect(
        "Filter by category",
        options=all_categories,
        default=all_categories,
    )

    if st.sidebar.button("Refresh KPIs"):
        # Any interaction triggers a rerun in Streamlit; no-op body is fine.
        pass

    # Top-level KPIs (based on recent window)
    st.subheader("Summary KPIs")
    if st.button("Refresh summary KPIs"):
        # Any interaction triggers a rerun in Streamlit; no-op body is fine.
        pass

    # For KPIs, only the "last N minutes" metrics honor category filter.
    # All-time totals remain global.
    kpis = get_summary_kpis(
        window_minutes=window_minutes,
        categories=selected_categories if selected_categories else None,
    )
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Orders (all time)", f"{kpis['total_orders']:,}")
    col2.metric("Total Revenue (all time)", f"${kpis['total_revenue']:,.2f}")
    col3.metric(
        f"Orders (last {window_minutes} min)", f"{kpis['orders_window']:,}"
    )
    col4.metric(
        f"Revenue (last {window_minutes} min)",
        f"${kpis['revenue_window']:,.2f}",
    )

    # Secondary KPI row
    k2col1, k2col2, k2col3, k2col4 = st.columns(4)
    k2col1.metric(
        f"Active customers (last {window_minutes} min)",
        f"{kpis['active_customers_window']:,}",
    )
    k2col2.metric(
        "Avg order value (all time)",
        f"${kpis['avg_order_value_all']:,.2f}",
    )
    k2col3.metric(
        f"Avg order value (last {window_minutes} min)",
        f"${kpis['avg_order_value_window']:,.2f}",
    )
    k2col4.metric(
        f"Orders per min (last {window_minutes} min)",
        f"{kpis['orders_per_min_window']:,.2f}",
    )

    st.markdown("---")

    # Middle row: sales by category + inventory snapshot (charts)
    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader(f"Sales by Category (last {window_minutes} min)")
        category_rows = get_sales_by_category(
            window_minutes=window_minutes,
            categories=selected_categories if selected_categories else None,
        )
        if category_rows:
            df_cat = pd.DataFrame(category_rows)
            bar_cat = alt.Chart(df_cat).mark_bar().encode(
                x=alt.X("category:N", sort="-y", title="Category"),
                y=alt.Y("revenue:Q", title="Revenue"),
                color=alt.Color(
                    "category:N",
                    legend=None,
                    scale=alt.Scale(range=SNOWFLAKE_PALETTE),
                ),
                tooltip=[
                    alt.Tooltip("category:N", title="Category"),
                    alt.Tooltip("items_sold:Q", title="Items sold"),
                    alt.Tooltip("revenue:Q", title="Revenue"),
                ],
            )
            text_cat = alt.Chart(df_cat).mark_text(
                align="center",
                baseline="bottom",
                dy=-4,
            ).encode(
                x=alt.X("category:N", sort="-y"),
                y=alt.Y("revenue:Q"),
                text=alt.Text("revenue:Q", format=".0f"),
            )
            chart_cat = (bar_cat + text_cat).properties(height=300)
            st.altair_chart(chart_cat, use_container_width=True)
        else:
            st.info("No sales data yet. Start the ingestion script to see activity.")

    with right_col:
        st.subheader("Inventory Snapshot")
        inventory_rows = get_inventory_snapshot(
            categories=selected_categories if selected_categories else None
        )
        if inventory_rows:
            df_inv = pd.DataFrame(inventory_rows)

            bar_inv = alt.Chart(df_inv).mark_bar().encode(
                x=alt.X("product_name:N", sort=None, title="Product"),
                y=alt.Y("stock:Q", title="Stock"),
                color=alt.Color(
                    "category:N",
                    title="Category",
                    scale=alt.Scale(range=SNOWFLAKE_PALETTE),
                ),
                tooltip=[
                    alt.Tooltip("product_name:N", title="Product"),
                    alt.Tooltip("category:N", title="Category"),
                    alt.Tooltip("stock:Q", title="Stock"),
                ],
            )
            text_inv = alt.Chart(df_inv).mark_text(
                align="center",
                baseline="bottom",
                dy=-4,
            ).encode(
                x=alt.X("product_name:N", sort=None),
                y=alt.Y("stock:Q"),
                text=alt.Text("stock:Q", format=".0f"),
            )
            chart_inv = (bar_inv + text_inv).properties(height=300)
            st.altair_chart(chart_inv, use_container_width=True)
        else:
            st.info("Inventory not initialized. Run app.init_db first.")

    st.markdown("---")

    # Recent orders as chart
    st.subheader(f"Most Recent Orders (last {window_minutes} min)")
    recent_rows = get_recent_orders(
        limit=recent_limit,
        window_minutes=window_minutes,
        categories=selected_categories if selected_categories else None,
    )
    if recent_rows:
        df_recent = pd.DataFrame(recent_rows)
        # Ensure a stable ordering by order_id descending
        df_recent = df_recent.sort_values("order_id", ascending=False)

        bar_recent = alt.Chart(df_recent).mark_bar().encode(
            x=alt.X("order_id:O", sort="-x", title="Order ID"),
            y=alt.Y("total_cost:Q", title="Order value"),
            color=alt.Color(
                "category:N",
                title="Category",
                scale=alt.Scale(range=SNOWFLAKE_PALETTE),
            ),
            tooltip=[
                alt.Tooltip("order_id:O", title="Order ID"),
                alt.Tooltip("customer_name:N", title="Customer"),
                alt.Tooltip("product_name:N", title="Product"),
                alt.Tooltip("category:N", title="Category"),
                alt.Tooltip("quantity:Q", title="Qty"),
                alt.Tooltip("total_cost:Q", title="Value"),
            ],
        )
        text_recent = alt.Chart(df_recent).mark_text(
            align="center",
            baseline="bottom",
            dy=-4,
        ).encode(
            x=alt.X("order_id:O", sort="-x"),
            y=alt.Y("total_cost:Q"),
            text=alt.Text("total_cost:Q", format=".0f"),
        )
        chart_recent = (bar_recent + text_recent).properties(height=350)
        st.altair_chart(chart_recent, use_container_width=True)
    else:
        st.info("No orders yet. Start the continuous ingestion script.")

    st.markdown("---")

    # Additional KPI dashboards: revenue over time and top customers
    st.subheader(f"Additional KPIs (last {window_minutes} min)")
    extra_left, extra_right = st.columns(2)

    with extra_left:
        st.markdown("**Revenue & Orders Over Time**")
        ts_rows = get_revenue_timeseries(
            window_minutes=window_minutes,
            categories=selected_categories if selected_categories else None,
        )
        if ts_rows:
            df_ts = pd.DataFrame(ts_rows)
            # Line for revenue, bars for orders
            line_rev = (
                alt.Chart(df_ts)
                .mark_line(point=True)
                .encode(
                    x=alt.X("ts:T", title="Time"),
                    y=alt.Y("revenue:Q", title="Revenue"),
                    color=alt.value(SNOWFLAKE_PALETTE[0]),
                    tooltip=[
                        alt.Tooltip("ts:T", title="Time"),
                        alt.Tooltip("revenue:Q", title="Revenue"),
                        alt.Tooltip("orders:Q", title="Orders"),
                    ],
                )
            )
            bar_orders = (
                alt.Chart(df_ts)
                .mark_bar(opacity=0.3)
                .encode(
                    x=alt.X("ts:T", title="Time"),
                    y=alt.Y("orders:Q", title="Orders"),
                    tooltip=[
                        alt.Tooltip("ts:T", title="Time"),
                        alt.Tooltip("orders:Q", title="Orders"),
                    ],
                    color=alt.value(SNOWFLAKE_PALETTE[1]),
                )
            )
            chart_ts = (
                (bar_orders + line_rev)
                .resolve_scale(y="independent")
                .properties(height=300)
            )
            st.altair_chart(chart_ts, use_container_width=True)
        else:
            st.info("No payments in this time window.")

    with extra_right:
        st.markdown("**Top Customers by Revenue**")
        top_rows = get_top_customers(
            window_minutes=window_minutes,
            limit=10,
            categories=selected_categories if selected_categories else None,
        )
        if top_rows:
            df_top = pd.DataFrame(top_rows)
            bar_top = (
                alt.Chart(df_top)
                .mark_bar()
                .encode(
                    x=alt.X("revenue:Q", title="Revenue"),
                    y=alt.Y(
                        "customer_name:N",
                        sort="-x",
                        title="Customer",
                    ),
                    color=alt.value(SNOWFLAKE_PALETTE[2]),
                    tooltip=[
                        alt.Tooltip("customer_name:N", title="Customer"),
                        alt.Tooltip("orders:Q", title="Orders"),
                        alt.Tooltip("revenue:Q", title="Revenue"),
                    ],
                )
                .properties(height=300)
            )
            st.altair_chart(bar_top, use_container_width=True)
        else:
            st.info("No customer activity in this time window.")


if __name__ == "__main__":
    main()


