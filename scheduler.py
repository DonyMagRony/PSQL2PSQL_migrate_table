import psycopg2
from worker import process_batch
import os

def create_batches():
    conn = psycopg2.connect(os.environ['SOURCE_DB_URL'])
    cur = conn.cursor()

    cur.execute("TRUNCATE batch_metadata RESTART IDENTITY")

    cur.execute("SELECT DISTINCT user_id FROM recommendation ORDER BY user_id")
    user_ids = [row[0] for row in cur.fetchall()]

    batch_size = 100
    user_batches = [
        user_ids[i:i + batch_size]
        for i in range(0, len(user_ids), batch_size)
    ]

    for user_batch in user_batches:
        cur.execute("""
            INSERT INTO batch_metadata (user_ids)
            VALUES (%s)
            RETURNING batch_id
        """, (user_batch,))
        batch_id = cur.fetchone()[0]
        process_batch.delay(batch_id, user_batch)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == '__main__':
    create_batches()