-- 1. Bảng Khách hàng
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL, 
    full_name VARCHAR(100),
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    gender VARCHAR(10) CHECK (gender IN ('Male', 'Female', 'Other')),
    location VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- 2. Bảng Sản phẩm 
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    brand VARCHAR(50) NOT NULL, 
    category VARCHAR(50),
    price INT NOT NULL,
    image_url VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- 3. Bảng Tồn kho 
CREATE TABLE IF NOT EXISTS inventory (
    product_id VARCHAR(50) PRIMARY KEY REFERENCES products(product_id),
    import_price INT NOT NULL,
    stock_quantity INT NOT NULL,
    last_restock_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Bảng Mã giảm giá 
CREATE TABLE IF NOT EXISTS promotions (
    promo_id VARCHAR(50) PRIMARY KEY,
    promo_code VARCHAR(50) UNIQUE NOT NULL,
    discount_percent DECIMAL(5,2) CHECK (discount_percent >= 0 AND discount_percent <= 100),
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- 5. Bảng Đơn hàng
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES users(user_id),
    promo_id VARCHAR(50) REFERENCES promotions(promo_id), -- Có thể NULL nếu không dùng mã
    subtotal INT NOT NULL,     -- Tiền hàng trước giảm giá
    shipping_fee INT DEFAULT 0, -- Phí giao hàng
    discount_amount INT DEFAULT 0, -- Số tiền được giảm
    total_amount INT NOT NULL, -- Tổng tiền cuối cùng khách phải trả
    payment_method VARCHAR(50) CHECK (payment_method IN ('COD', 'Credit Card', 'E-Wallet', 'Bank Transfer')),
    order_status VARCHAR(50) DEFAULT 'Pending' CHECK (order_status IN ('Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled', 'Returned')),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. Bảng Chi tiết Đơn hàng 
CREATE TABLE IF NOT EXISTS order_items (
    item_id SERIAL PRIMARY KEY,
    order_id VARCHAR(50) REFERENCES orders(order_id),
    product_id VARCHAR(50) REFERENCES products(product_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    unit_price INT NOT NULL, -- Giá bán tại thời điểm đó
    unit_cost INT NOT NULL,  -- Giá vốn tại thời điểm đó (Lấy từ inventory để tính Biên lợi nhuận - Profit Margin)
    item_total INT NOT NULL
);

-- 7. Bảng Lịch sử kho
CREATE TABLE IF NOT EXISTS inventory_logs (
    log_id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) REFERENCES products(product_id),
    order_id VARCHAR(50), 
    transaction_type VARCHAR(10) CHECK (transaction_type IN ('IN', 'OUT', 'RETURN')),
    quantity_changed INT NOT NULL, 
    note TEXT, -- Ghi chú (VD: "Nhập hàng tháng 5", "Hàng hoàn trả")
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 8. Bảng Đánh giá sản phẩm (MỚI - Cung cấp data cho bài toán Text Classification / Sentiment Analysis sau này)
CREATE TABLE IF NOT EXISTS reviews (
    review_id SERIAL PRIMARY KEY,
    product_id VARCHAR(50) REFERENCES products(product_id),
    user_id VARCHAR(50) REFERENCES users(user_id),
    rating INT CHECK (rating >= 1 AND rating <= 5),
    comment TEXT,
    review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);



-- ==============================================================================
-- INSERT USERS
-- ==============================================================================

INSERT INTO users (user_id, username,password, full_name, email, phone, gender, location, is_active) VALUES
('U001', 'khoi', '123', 'Lê Nguyên Khôi', 'khoi@gmail.com', '0901234567', 'Male', 'Hồ Chí Minh', TRUE),
('U002', 'admin', 'admin', 'Quản trị viên', 'admin@gmail.com', '0911111111', 'Male', 'Hà Nội', TRUE),
('U003', 'guest', '123', 'Khách hàng', 'guest@gmail.com', '0922222222', 'Other', 'Đà Nẵng', TRUE)
ON CONFLICT (user_id) DO NOTHING;


-- ==============================================================================
-- INSERT PRODUCTS
-- ==============================================================================

INSERT INTO products (product_id,product_name,brand,category,price,image_url,is_active) VALUES
-- SMARTPHONE
('P001', 'iPhone 15 Pro', 'Apple', 'Smartphone', 28000000, 'DT01.png', TRUE),
('P002', 'Samsung Galaxy S24 Ultra', 'Samsung', 'Smartphone', 27000000, 'DT02.png', TRUE),
('P003', 'Xiaomi 14', 'Xiaomi', 'Smartphone', 20000000, 'DT03.png', TRUE),
('P004', 'Oppo Find X7', 'Oppo', 'Smartphone', 19000000, 'DT04.png', TRUE),
('P005', 'Vivo X100 Pro', 'Vivo', 'Smartphone', 21000000, 'DT05.png', TRUE),
('P006', 'Realme GT5', 'Realme', 'Smartphone', 15000000, 'DT06.png', TRUE),
('P007', 'Asus ROG Phone 8', 'Asus', 'Smartphone', 25000000, 'DT07.png', TRUE),
('P008', 'Huawei P60 Pro', 'Huawei', 'Smartphone', 23000000, 'DT08.png', TRUE),
('P009', 'Tecno Camon 30', 'Tecno', 'Smartphone', 7000000, 'DT09.png', TRUE),
('P010', 'Nokia G60 5G', 'Nokia', 'Smartphone', 6000000, 'DT10.png', TRUE),

-- LAPTOP
('P011', 'MacBook Air M2', 'Apple', 'Laptop', 27000000, 'L001.png', TRUE),
('P012', 'Dell XPS 13 Plus', 'Dell', 'Laptop', 33000000, 'L002.png', TRUE),
('P013', 'HP Spectre x360', 'HP', 'Laptop', 31000000, 'L003.png', TRUE),
('P014', 'Asus ZenBook 14 OLED', 'Asus', 'Laptop', 25000000, 'L004.png', TRUE),
('P015', 'Lenovo ThinkPad X1 Carbon', 'Lenovo', 'Laptop', 38000000, 'L005.png', TRUE),
('P016', 'MSI GF63 Thin', 'MSI', 'Laptop', 19000000, 'L006.png', TRUE),
('P017', 'Acer Aspire 5', 'Acer', 'Laptop', 14000000, 'L007.png', TRUE),
('P018', 'Gigabyte G5', 'Gigabyte', 'Laptop', 22000000, 'L008.png', TRUE),
('P019', 'LG Gram 16', 'LG', 'Laptop', 32000000, 'L009.png', TRUE),
('P020', 'Huawei MateBook D16', 'Huawei', 'Laptop', 20000000, 'L010.png', TRUE),

-- TABLET
('P021', 'iPad Air 5', 'Apple', 'Tablet', 16000000, 'MT001.png', TRUE),
('P022', 'Samsung Galaxy Tab S9', 'Samsung', 'Tablet', 18000000, 'MT002.png', TRUE),
('P023', 'Xiaomi Pad 6', 'Xiaomi', 'Tablet', 9000000, 'MT003.png', TRUE),
('P024', 'Lenovo Tab P12 Pro', 'Lenovo', 'Tablet', 12000000, 'MT004.png', TRUE),
('P025', 'Huawei MatePad 11', 'Huawei', 'Tablet', 13000000, 'MT005.png', TRUE),
('P026', 'Realme Pad X', 'Realme', 'Tablet', 8000000, 'MT006.png', TRUE),
('P027', 'Oppo Pad 2', 'Oppo', 'Tablet', 15000000, 'MT007.png', TRUE),
('P028', 'TCL Tab 10s', 'TCL', 'Tablet', 5000000, 'MT008.png', TRUE),
('P029', 'Alldocube iPlay 50', 'Alldocube', 'Tablet', 4000000, 'MT009.png', TRUE),
('P030', 'Amazon Fire HD 10', 'Amazon', 'Tablet', 6000000, 'MT0010.png', TRUE),

-- TAI NGHE
('P031', 'AirPods Pro 2', 'Apple', 'TaiNghe', 6500000, 'T001.png', TRUE),
('P032', 'Sony WH-1000XM5', 'Sony', 'TaiNghe', 7000000, 'T002.png', TRUE),
('P033', 'JBL Tune 760NC', 'JBL', 'TaiNghe', 2500000, 'T003.png', TRUE),
('P034', 'Samsung Galaxy Buds2 Pro', 'Samsung', 'TaiNghe', 4800000, 'T004.png', TRUE),
('P035', 'Xiaomi Redmi Buds 5 Pro', 'Xiaomi', 'TaiNghe', 1800000, 'T005.png', TRUE),
('P036', 'Anker Soundcore Life Q35', 'Anker', 'TaiNghe', 3200000, 'T006.png', TRUE),
('P037', 'Sennheiser Momentum 4', 'Sennheiser', 'TaiNghe', 9500000, 'T007.png', TRUE),
('P038', 'Bose QuietComfort Ultra', 'Bose', 'TaiNghe', 8900000, 'T008.png', TRUE),
('P039', 'Edifier W820NB', 'Edifier', 'TaiNghe', 1700000, 'T009.png', TRUE),
('P040', 'Beats Studio Pro', 'Beats', 'TaiNghe', 8900000, 'T010.png', TRUE)

ON CONFLICT (product_id) DO NOTHING;


-- ==============================================================================
-- INSERT INVENTORY
-- ==============================================================================

INSERT INTO inventory (product_id, import_price, stock_quantity) 
SELECT
    product_id,
    (price * 0.7)::INT,
    FLOOR(RANDOM() * 40 + 10)::INT
FROM products
ON CONFLICT (product_id) DO NOTHING;

-- INSERT PROMTIONS
INSERT INTO promotions (promo_id, promo_code, discount_percent, start_date, end_date, is_active)VALUES
('PROMO001', 'SAVE10', 10.00, NOW() - INTERVAL '10 days', NOW() + INTERVAL '30 days',TRUE),
('PROMO002','WELCOME10',10.00,NOW() - INTERVAL '5 days',NOW() + INTERVAL '60 days',TRUE),
('PROMO003','SPRING10',10.00,NOW() - INTERVAL '20 days',NOW() + INTERVAL '15 days',TRUE);