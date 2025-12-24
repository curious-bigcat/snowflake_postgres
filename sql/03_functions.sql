-- Stored function to encapsulate transactional checkout logic

CREATE OR REPLACE FUNCTION place_order(
    p_customer_id INT,
    p_product_id  INT,
    p_quantity    INT
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    v_price     DECIMAL(10, 2);
    v_available INT;
    v_order_id  INT;
BEGIN
    -- Lock the inventory row to prevent overselling
    SELECT stock
    INTO v_available
    FROM inventory
    WHERE product_id = p_product_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Product % not found', p_product_id;
    END IF;

    IF v_available < p_quantity THEN
        RAISE EXCEPTION 'Insufficient stock. Requested %, available %',
                        p_quantity, v_available;
    END IF;

    -- Get current price
    SELECT price
    INTO v_price
    FROM products
    WHERE product_id = p_product_id;

    -- Create order
    INSERT INTO orders (order_date, customer_id, product_id, quantity)
    VALUES (CURRENT_DATE, p_customer_id, p_product_id, p_quantity)
    RETURNING order_id INTO v_order_id;

    -- Record payment (assume authorized)
    INSERT INTO payments (order_id, amount, status)
    VALUES (v_order_id, v_price * p_quantity, 'AUTHORIZED');

    -- Decrement inventory
    UPDATE inventory
    SET stock = stock - p_quantity
    WHERE product_id = p_product_id;

    RETURN v_order_id;
END;
$$;


