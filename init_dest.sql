CREATE TABLE recommendation (
    id SERIAL PRIMARY KEY,
    user_id INT,
    product_id INT,
    created_at TIMESTAMP DEFAULT NOW()
);