import psycopg2
from worker import process_batch
import os


def create_batches():
    conn = psycopg2.connect(os.environ['SOURCE_DB_URL'])
    cur = conn.cursor()

    # Clear existing batches (for testing)
    cur.execute("TRUNCATE batch_metadata RESTART IDENTITY")

    # Get max ID from recommendation table
    cur.execute("SELECT MAX(id) FROM recommendation")
    max_id = cur.fetchone()[0] or 0
    batch_size = 10000  # Adjustable

    # Generate batches
    batches = [(i, min(i + batch_size - 1, max_id)) for i in range(1, max_id + 1, batch_size)]

    # Insert batches into metadata and enqueue tasks
    for start, end in batches:
        cur.execute("""
            INSERT INTO batch_metadata (start_id, end_id)
            VALUES (%s, %s)
            RETURNING batch_id
        """, (start, end))
        batch_id = cur.fetchone()[0]
        process_batch.delay(batch_id, start, end)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == '__main__':
    create_batches()