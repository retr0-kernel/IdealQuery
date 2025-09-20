-- Initialize OptiQuery test database
-- Connect to the new database
\c optiquery_test;

-- Create customers table
CREATE TABLE IF NOT EXISTS customers (
                                         customer_id SERIAL PRIMARY KEY,
                                         name VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE,
    age INTEGER,
    city VARCHAR(50),
    country VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

-- Create orders table
CREATE TABLE IF NOT EXISTS orders (
                                      order_id SERIAL PRIMARY KEY,
                                      customer_id INTEGER REFERENCES customers(customer_id),
    product_name VARCHAR(200),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(12,2),
    order_date DATE,
    status VARCHAR(20)
    );

-- Create products table
CREATE TABLE IF NOT EXISTS products (
                                        product_id SERIAL PRIMARY KEY,
                                        name VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock_quantity INTEGER,
    supplier_id INTEGER
    );

-- Create suppliers table
CREATE TABLE IF NOT EXISTS suppliers (
                                         supplier_id SERIAL PRIMARY KEY,
                                         name VARCHAR(100) NOT NULL,
    contact_email VARCHAR(150),
    country VARCHAR(50),
    rating DECIMAL(2,1)
    );

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_customers_email ON customers(email);
CREATE INDEX IF NOT EXISTS idx_customers_country ON customers(country);
CREATE INDEX IF NOT EXISTS idx_customers_age ON customers(age);

CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);

CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
CREATE INDEX IF NOT EXISTS idx_products_supplier ON products(supplier_id);

-- Insert sample data
INSERT INTO customers (name, email, age, city, country) VALUES
                                                            ('John Doe', 'john@example.com', 30, 'New York', 'USA'),
                                                            ('Jane Smith', 'jane@example.com', 25, 'London', 'UK'),
                                                            ('Bob Johnson', 'bob@example.com', 35, 'Toronto', 'Canada'),
                                                            ('Alice Brown', 'alice@example.com', 28, 'Sydney', 'Australia'),
                                                            ('Charlie Wilson', 'charlie@example.com', 32, 'Berlin', 'Germany');

INSERT INTO suppliers (name, contact_email, country, rating) VALUES
                                                                 ('Tech Corp', 'tech@corp.com', 'USA', 4.5),
                                                                 ('Global Electronics', 'global@electronics.com', 'China', 4.2),
                                                                 ('Euro Supplies', 'euro@supplies.com', 'Germany', 4.7);

INSERT INTO products (name, category, price, stock_quantity, supplier_id) VALUES
                                                                              ('Laptop Pro', 'Electronics', 1299.99, 50, 1),
                                                                              ('Wireless Mouse', 'Electronics', 29.99, 200, 1),
                                                                              ('Office Chair', 'Furniture', 299.99, 30, 2),
                                                                              ('Desk Lamp', 'Furniture', 79.99, 100, 3),
                                                                              ('Smartphone', 'Electronics', 799.99, 75, 2);

INSERT INTO orders (customer_id, product_name, quantity, unit_price, total_amount, order_date, status) VALUES
                                                                                                           (1, 'Laptop Pro', 1, 1299.99, 1299.99, '2023-01-15', 'completed'),
                                                                                                           (2, 'Wireless Mouse', 2, 29.99, 59.98, '2023-01-16', 'completed'),
                                                                                                           (3, 'Office Chair', 1, 299.99, 299.99, '2023-01-17', 'pending'),
                                                                                                           (1, 'Smartphone', 1, 799.99, 799.99, '2023-01-18', 'completed'),
                                                                                                           (4, 'Desk Lamp', 3, 79.99, 239.97, '2023-01-19', 'shipped');

-- Update statistics
ANALYZE customers;
ANALYZE orders;
ANALYZE products;
ANALYZE suppliers;
