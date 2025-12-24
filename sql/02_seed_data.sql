-- Seed data for customers, products, and initial inventory

-- Customers
INSERT INTO customers (first_name, last_name, email, city) VALUES
('Alice', 'Smith', 'alice@example.com', 'New York'),
('Bob', 'Johnson', 'bob@test.com', 'Los Angeles'),
('Charlie', 'Brown', 'charlie@sample.net', 'Chicago'),
('Diana', 'Prince', 'diana@themyscira.com', 'Houston'),
('Evan', 'Wright', 'evan@write.com', 'Phoenix'),
('Fiona', 'Gallagher', 'fiona@shameless.com', 'Chicago'),
('George', 'Martin', 'grrm@books.com', 'Santa Fe'),
('Hannah', 'Montana', 'hannah@music.com', 'Los Angeles'),
('Ian', 'Malcolm', 'ian@chaos.com', 'Austin'),
('Julia', 'Roberts', 'julia@movie.com', 'New York');

-- Products
INSERT INTO products (product_name, category, price) VALUES
('Wireless Mouse', 'Electronics', 25.99),
('Mechanical Keyboard', 'Electronics', 120.50),
('Gaming Monitor', 'Electronics', 300.00),
('Yoga Mat', 'Fitness', 20.00),
('Dumbbell Set', 'Fitness', 55.00),
('Running Shoes', 'Footwear', 89.99),
('Leather Jacket', 'Apparel', 150.00),
('Coffee Maker', 'Kitchen', 45.00),
('Blender', 'Kitchen', 30.00),
('Novel: The Great Gatsby', 'Books', 12.50);

-- Initial inventory (e.g., 100 units of each product)
-- Use ON CONFLICT to make this seed step idempotent when re-running init_db.
INSERT INTO inventory (product_id, stock)
SELECT product_id, 100
FROM products
ON CONFLICT (product_id) DO NOTHING;


