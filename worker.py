from celery import Celery
import psycopg2
from io import StringIO
import os

app = Celery('worker', broker=os.environ['REDIS_URL'], backend=os.environ['REDIS_URL'])


@app.task(bind=True, max_retries=3)
def process_batch(self, batch_id, start_id, end_id):
    source_conn = None
    dest_conn = None
    try:
        # Connect to databases
        source_conn = psycopg2.connect(os.environ['SOURCE_DB_URL'])
        dest_conn = psycopg2.connect(os.environ['DEST_DB_URL'])
        source_cur = source_conn.cursor()

        # Update batch status to 'in_progress'
        source_cur.execute("""
            UPDATE batch_metadata 
            SET status = 'in_progress', started_at = NOW() 
            WHERE batch_id = %s
        """, (batch_id,))
        source_conn.commit()

        # Export data from source to buffer
        buffer = StringIO()
        source_cur.copy_expert(
            f"COPY (SELECT * FROM recommendation WHERE id BETWEEN {start_id} AND {end_id}) TO STDOUT WITH CSV HEADER",
            buffer
        )
        buffer.seek(0)

        # Import data into destination
        dest_cur = dest_conn.cursor()
        dest_cur.copy_expert("COPY recommendation FROM STDIN WITH CSV HEADER", buffer)
        dest_conn.commit()

        # Update batch status to 'completed'
        source_cur.execute("""
            UPDATE batch_metadata 
            SET status = 'completed', completed_at = NOW() 
            WHERE batch_id = %s
        """, (batch_id,))
        source_conn.commit()

    except Exception as e:
        if source_conn:
            source_conn.rollback()
            source_cur.execute("""
                UPDATE batch_metadata 
                SET status = 'failed', error_message = %s 
                WHERE batch_id = %s
            """, (str(e), batch_id))
            source_conn.commit()
        self.retry(exc=e, countdown=2 ** self.request.retries)
    finally:
        if source_conn: source_conn.close()
        if dest_conn: dest_conn.close()