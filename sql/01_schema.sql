-- Schema for transactional Snowflake Postgres demo (e-commerce checkout)

-- Customers table
CREATE TABLE IF NOT EXISTS customers (
    customer_id SERIAL PRIMARY KEY,
    first_name  VARCHAR(50),
    last_name   VARCHAR(50),
    email       VARCHAR(100),
    city        VARCHAR(50)
);

-- Products table
CREATE TABLE IF NOT EXISTS products (
    product_id   SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category     VARCHAR(50),
    price        DECIMAL(10, 2)
);

-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    order_id    SERIAL PRIMARY KEY,
    order_date  DATE DEFAULT CURRENT_DATE,
    customer_id INT,
    product_id  INT,
    quantity    INT,

    CONSTRAINT fk_order_customer
        FOREIGN KEY (customer_id) 
        REFERENCES customers(customer_id),

    CONSTRAINT fk_order_product
        FOREIGN KEY (product_id) 
        REFERENCES products(product_id)
);

-- Inventory table
CREATE TABLE IF NOT EXISTS inventory (
    product_id INT PRIMARY KEY
        REFERENCES products(product_id),
    stock      INT NOT NULL,
    reserved   INT NOT NULL DEFAULT 0
);

-- Payments table
CREATE TABLE IF NOT EXISTS payments (
    payment_id SERIAL PRIMARY KEY,
    order_id   INT NOT NULL
        REFERENCES orders(order_id),
    amount     DECIMAL(10, 2) NOT NULL,
    status     VARCHAR(20) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Helpful indexes for OLTP style access
CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_order_date  ON orders (order_date);
CREATE INDEX IF NOT EXISTS idx_payments_order_id  ON payments (order_id);


