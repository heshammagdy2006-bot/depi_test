
-- =====================================================
-- Restaurant Chain Data Warehouse
-- Star Schema: Sales Fact Table
-- Grain: One row per menu item per customer order
-- =====================================================


-- 1. CREATE DATABASE
CREATE DATABASE IF NOT EXISTS datawarehouse;

USE datawarehouse;


-- =====================================================
-- 2. DIMENSION TABLES
-- =====================================================


-- 2.1 Dim_Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE,
    day_name VARCHAR(20),
    month_name VARCHAR(20),
    quarter INT,
    year INT
);


-- 2.2 Dim_Time
CREATE TABLE IF NOT EXISTS dim_time (
    time_key INT PRIMARY KEY,
    full_time TIME,
    hour INT,
    minute INT,
    day_period VARCHAR(20)
);


-- 2.3 Dim_Branch
CREATE TABLE IF NOT EXISTS dim_branch (
    branch_key INT AUTO_INCREMENT PRIMARY KEY,
    branch_id INT,
    branch_name VARCHAR(100),
    city VARCHAR(100),
    region VARCHAR(100)
);


-- 2.4 Dim_Customer
CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT,
    customer_name VARCHAR(100),
    registration_date DATE
);


-- 2.5 Dim_Menu_Item
CREATE TABLE IF NOT EXISTS dim_menu_item (
    item_key INT AUTO_INCREMENT PRIMARY KEY,
    item_id INT,
    item_name VARCHAR(100),
    category VARCHAR(100),
    cuisine VARCHAR(100)
);


-- =====================================================
-- 3. FACT TABLE
-- =====================================================


CREATE TABLE IF NOT EXISTS fact_sales (

    -- Primary Key
    sales_key BIGINT AUTO_INCREMENT PRIMARY KEY,

    -- Degenerate Dimension
    order_id BIGINT NOT NULL,
    order_line_number INT NOT NULL,

    -- Foreign Keys
    date_key INT,
    time_key INT,
    branch_key INT,
    customer_key INT NOT NULL DEFAULT 0,
    item_key INT,

    -- Measures
    quantity INT,
    unit_price DECIMAL(12,2),
    gross_sales_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    net_sales_amount DECIMAL(12,2),

    -- Foreign Key Relationships
    CONSTRAINT fk_sales_date
        FOREIGN KEY (date_key)
        REFERENCES dim_date(date_key),

    CONSTRAINT fk_sales_time
        FOREIGN KEY (time_key)
        REFERENCES dim_time(time_key),

    CONSTRAINT fk_sales_branch
        FOREIGN KEY (branch_key)
        REFERENCES dim_branch(branch_key),

    CONSTRAINT fk_sales_customer
        FOREIGN KEY (customer_key)
        REFERENCES dim_customer(customer_key),

    CONSTRAINT fk_sales_item
        FOREIGN KEY (item_key)
        REFERENCES dim_menu_item(item_key),

    -- Prevent duplicate order lines
    CONSTRAINT unique_order_line
        UNIQUE (order_id, order_line_number)

);


-- =====================================================
-- 4. INSERT GUEST CUSTOMER
-- customer_key = 0 means Guest
-- =====================================================


SET @OLD_SQL_MODE = @@SESSION.sql_mode;

SET SESSION sql_mode = CONCAT(
    @@SESSION.sql_mode,
    ',NO_AUTO_VALUE_ON_ZERO'
);

INSERT IGNORE INTO dim_customer (
    customer_key,
    customer_id,
    customer_name,
    registration_date
)
VALUES (
    0,
    NULL,
    'Guest',
    NULL
);

SET SESSION sql_mode = @OLD_SQL_MODE;


-- =====================================================
-- 5. INDEXES
-- Improve query performance
-- =====================================================


CREATE INDEX idx_sales_order
ON fact_sales(order_id);

CREATE INDEX idx_sales_date
ON fact_sales(date_key);

CREATE INDEX idx_sales_branch
ON fact_sales(branch_key);

CREATE INDEX idx_sales_customer
ON fact_sales(customer_key);

CREATE INDEX idx_sales_item
ON fact_sales(item_key);


-- =====================================================
-- 6. CHECK TABLES
-- =====================================================


SHOW TABLES;


-- Check Fact Table Structure
DESCRIBE fact_sales;


-- Check Dimensions
DESCRIBE dim_date;
DESCRIBE dim_time;
DESCRIBE dim_branch;
DESCRIBE dim_customer;
DESCRIBE dim_menu_item;


-- Check Guest Record
SELECT *
FROM dim_customer
WHERE customer_key = 0;