CREATE TABLE recommendation (
    id SERIAL PRIMARY KEY,
    user_id INT,
    product_id INT,
    score NUMERIC(5,4),
    created_at TIMESTAMP DEFAULT NOW()
);