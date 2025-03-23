CREATE TABLE recommendation (
    id SERIAL PRIMARY KEY,
    user_id INT,
    product_id INT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE batch_metadata (
    batch_id SERIAL PRIMARY KEY,
    start_id INT,
    end_id INT,
    status VARCHAR(20) DEFAULT 'pending',
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);

-- Function to insert data in batches
DO $$
DECLARE
    total_records INT := 1000000;  -- Total records to insert
    batch_size INT := 100000;     -- Records per batch
    current_batch INT := 0;
BEGIN
    WHILE current_batch * batch_size < total_records LOOP
        INSERT INTO recommendation (user_id, product_id)
        SELECT
            (random() * 1000000)::int AS user_id,  -- Random user_id between 1 and 1,000,000
            (random() * 100)::int AS product_id     -- Random product_id between 1 and 100
        FROM generate_series(1, batch_size);

        current_batch := current_batch + 1;
        RAISE NOTICE 'Inserted batch %: % records', current_batch, batch_size;
    END LOOP;
END $$;