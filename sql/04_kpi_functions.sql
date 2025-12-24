-- KPI and analytics helper functions for the Streamlit dashboard.
-- These encapsulate all SQL logic in Postgres so the UI can remain thin.

-- Summary KPIs for a given time window (and optional category filter)
CREATE OR REPLACE FUNCTION fn_summary_kpis(
    p_window_minutes INT,
    p_categories TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    total_orders BIGINT,
    total_revenue NUMERIC,
    orders_window BIGINT,
    revenue_window NUMERIC,
    active_customers_window BIGINT,
    avg_order_value_all NUMERIC,
    avg_order_value_window NUMERIC,
    orders_per_min_window NUMERIC
)
LANGUAGE plpgsql
AS $$
BEGIN
    -- All-time totals
    SELECT
        COUNT(*)::BIGINT,
        COALESCE(SUM(amount), 0)
    INTO total_orders, total_revenue
    FROM payments
    WHERE status = 'AUTHORIZED';

    -- Recent window, optionally filtered by product category
    SELECT
        COUNT(*)::BIGINT,
        COALESCE(SUM(pay.amount), 0),
        COUNT(DISTINCT o.customer_id)::BIGINT
    INTO orders_window, revenue_window, active_customers_window
    FROM payments pay
    JOIN orders o ON o.order_id = pay.order_id
    JOIN products p ON p.product_id = o.product_id
    WHERE pay.status = 'AUTHORIZED'
      AND pay.created_at >= NOW() - (p_window_minutes || ' minutes')::INTERVAL
      AND (p_categories IS NULL OR p.category = ANY (p_categories));

    IF total_orders > 0 THEN
        avg_order_value_all := total_revenue / total_orders;
    ELSE
        avg_order_value_all := 0;
    END IF;

    IF orders_window > 0 THEN
        avg_order_value_window := revenue_window / orders_window;
    ELSE
        avg_order_value_window := 0;
    END IF;

    IF p_window_minutes > 0 THEN
        orders_per_min_window := orders_window::NUMERIC / p_window_minutes;
    ELSE
        orders_per_min_window := 0;
    END IF;

    RETURN NEXT;
END;
$$;


-- Sales by category in recent window (optionally filtered by category list)
CREATE OR REPLACE FUNCTION fn_sales_by_category(
    p_window_minutes INT,
    p_categories TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    category VARCHAR,
    items_sold BIGINT,
    revenue NUMERIC
)
LANGUAGE sql
AS $$
    SELECT
        p.category,
        SUM(o.quantity)::BIGINT AS items_sold,
        SUM(p.price * o.quantity) AS revenue
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    JOIN payments pay ON pay.order_id = o.order_id
    WHERE pay.status = 'AUTHORIZED'
      AND pay.created_at >= NOW() - (p_window_minutes || ' minutes')::INTERVAL
      AND (p_categories IS NULL OR p.category = ANY (p_categories))
    GROUP BY p.category
    ORDER BY revenue DESC;
$$;


-- Inventory snapshot (optionally filtered by category list)
CREATE OR REPLACE FUNCTION fn_inventory_snapshot(
    p_categories TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    product_id INT,
    product_name VARCHAR,
    category VARCHAR,
    stock INT
)
LANGUAGE sql
AS $$
    SELECT
        p.product_id,
        p.product_name,
        p.category,
        i.stock
    FROM inventory i
    JOIN products p ON p.product_id = i.product_id
    WHERE (p_categories IS NULL OR p.category = ANY (p_categories))
    ORDER BY p.product_id;
$$;


-- Recent orders in window, limited, optionally filtered by categories
CREATE OR REPLACE FUNCTION fn_recent_orders(
    p_window_minutes INT,
    p_limit INT,
    p_categories TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    order_id INT,
    order_date DATE,
    customer_name VARCHAR,
    product_name VARCHAR,
    category VARCHAR,
    price NUMERIC,
    quantity INT,
    total_cost NUMERIC
)
LANGUAGE sql
AS $$
    SELECT
        o.order_id,
        o.order_date,
        c.first_name || ' ' || c.last_name AS customer_name,
        p.product_name,
        p.category,
        p.price,
        o.quantity,
        (p.price * o.quantity) AS total_cost
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN products p ON o.product_id = p.product_id
    JOIN payments pay ON pay.order_id = o.order_id
    WHERE pay.status = 'AUTHORIZED'
      AND pay.created_at >= NOW() - (p_window_minutes || ' minutes')::INTERVAL
      AND (p_categories IS NULL OR p.category = ANY (p_categories))
    ORDER BY o.order_id DESC
    LIMIT p_limit;
$$;


-- Revenue and orders over time (per minute) for recent window
CREATE OR REPLACE FUNCTION fn_revenue_timeseries(
    p_window_minutes INT,
    p_categories TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    ts TIMESTAMPTZ,
    revenue NUMERIC,
    orders BIGINT
)
LANGUAGE sql
AS $$
    SELECT
        date_trunc('minute', pay.created_at) AS ts,
        SUM(pay.amount) AS revenue,
        COUNT(*)::BIGINT AS orders
    FROM payments pay
    JOIN orders o ON o.order_id = pay.order_id
    JOIN products p ON p.product_id = o.product_id
    WHERE pay.status = 'AUTHORIZED'
      AND pay.created_at >= NOW() - (p_window_minutes || ' minutes')::INTERVAL
      AND (p_categories IS NULL OR p.category = ANY (p_categories))
    GROUP BY ts
    ORDER BY ts;
$$;


-- Top customers by revenue in recent window
CREATE OR REPLACE FUNCTION fn_top_customers(
    p_window_minutes INT,
    p_limit INT,
    p_categories TEXT[] DEFAULT NULL
)
RETURNS TABLE (
    customer_id INT,
    customer_name VARCHAR,
    revenue NUMERIC,
    orders BIGINT
)
LANGUAGE sql
AS $$
    SELECT
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        SUM(pay.amount) AS revenue,
        COUNT(*)::BIGINT AS orders
    FROM payments pay
    JOIN orders o ON o.order_id = pay.order_id
    JOIN customers c ON c.customer_id = o.customer_id
    JOIN products p ON p.product_id = o.product_id
    WHERE pay.status = 'AUTHORIZED'
      AND pay.created_at >= NOW() - (p_window_minutes || ' minutes')::INTERVAL
      AND (p_categories IS NULL OR p.category = ANY (p_categories))
    GROUP BY c.customer_id, customer_name
    ORDER BY revenue DESC
    LIMIT p_limit;
$$;


