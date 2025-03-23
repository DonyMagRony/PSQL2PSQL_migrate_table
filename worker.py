from celery import Celery
import psycopg2
from psycopg2 import pool
from io import StringIO
import os

source_pool = None
dest_pool = None

def create_pools():
    global source_pool, dest_pool
    source_pool = pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        dsn=os.environ['SOURCE_DB_URL']
    )
    dest_pool = pool.ThreadedConnectionPool(
        minconn=1,
        maxconn=20,
        dsn=os.environ['DEST_DB_URL']
    )

app = Celery('worker.app', broker=os.environ['REDIS_URL'], backend=os.environ['REDIS_URL'])

@app.task(bind=True, max_retries=3)
def process_batch(self, batch_id, user_ids):
    source_conn = None
    dest_conn = None
    try:

        source_conn = source_pool.getconn()
        dest_conn = dest_pool.getconn()
        source_cur = source_conn.cursor()

        source_cur.execute("""
            UPDATE batch_metadata 
            SET status = 'in_progress', started_at = NOW() 
            WHERE batch_id = %s
        """, (batch_id,))
        source_conn.commit()

        buffer = StringIO()
        query = f"""
            COPY (
                SELECT * FROM recommendation 
                WHERE user_id IN ({','.join(map(str, user_ids))})
                ORDER BY id
            ) TO STDOUT WITH CSV HEADER
        """
        source_cur.copy_expert(query, buffer)
        buffer.seek(0)

        dest_cur = dest_conn.cursor()
        dest_cur.copy_expert("COPY recommendation FROM STDIN WITH CSV HEADER", buffer)
        dest_conn.commit()

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
        if source_conn:
            source_pool.putconn(source_conn)
        if dest_conn:
            dest_pool.putconn(dest_conn)

create_pools()