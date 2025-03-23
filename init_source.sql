CREATE TABLE recommendation (
    id SERIAL PRIMARY KEY,
    user_id INT,
    product_id INT,
    score NUMERIC(5,4),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE batch_metadata (
    batch_id SERIAL PRIMARY KEY,
    user_ids INTEGER[],          -- Array of user IDs
    status VARCHAR(20) DEFAULT 'pending',
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);

-- Function to insert data in batches
DO $$
DECLARE
    total_users INT := 1000000;  -- 1M users
    recs_per_user INT := 100;    -- 100 recs per user
    users_per_batch INT := 10000; -- 10K users/batch → 100K recs/batch
    current_batch INT := 0;
BEGIN
    FOR current_batch IN 0..(total_users/users_per_batch - 1) LOOP
        INSERT INTO recommendation (user_id, product_id, score)
        SELECT
            user_id,
            (random() * 100)::int AS product_id,
            (random() * 1)::NUMERIC(5,4) AS score
        FROM
            generate_series(
                current_batch * users_per_batch + 1,
                (current_batch + 1) * users_per_batch
            ) AS user_id
        CROSS JOIN generate_series(1, recs_per_user);

        RAISE NOTICE 'Inserted batch %: users %-% → % records',
            current_batch + 1,
            current_batch * users_per_batch + 1,
            (current_batch + 1) * users_per_batch,
            users_per_batch * recs_per_user;
    END LOOP;
END $$;


CREATE INDEX recommendation_user_id_idx ON recommendation (user_id);