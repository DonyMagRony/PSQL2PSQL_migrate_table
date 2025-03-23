# retry_retriable.py
import psycopg2
from psycopg2.extras import DictCursor
from worker import process_batch
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("retry_retriable")


def get_retriable_batches():
    """Получает батчи для ретрая (failed/pending/in_progress)"""
    conn = psycopg2.connect(os.environ['SOURCE_DB_URL'])
    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                SELECT batch_id, user_ids 
                FROM batch_metadata 
                WHERE status IN ('failed', 'pending', 'in_progress')
            """)
            return cur.fetchall()
    finally:
        conn.close()


def retry_batches(batches):
    """Обрабатывает retriable батчи"""
    for batch in batches:
        batch_id = batch['batch_id']
        user_ids = batch['user_ids']
        current_status = get_batch_status(batch_id)

        if current_status == 'completed':
            logger.info(f"Skipping COMPLETED batch {batch_id}")
            continue

        logger.info(f"Retrying {current_status.upper()} batch {batch_id}...")
        prepare_for_retry(batch_id, current_status)
        process_batch.delay(batch_id, user_ids)


def prepare_for_retry(batch_id, original_status):
    """Подготавливает батч к ретраю"""
    conn = psycopg2.connect(os.environ['SOURCE_DB_URL'])
    try:
        with conn.cursor() as cur:
            if original_status == 'in_progress':
                cur.execute("""
                    UPDATE batch_metadata
                    SET 
                        status = 'pending',
                        started_at = NULL,
                        error_message = 'Manual retry from in_progress'
                    WHERE batch_id = %s
                """, (batch_id,))
            else:
                cur.execute("""
                    UPDATE batch_metadata
                    SET 
                        status = 'pending',
                        error_message = NULL
                    WHERE batch_id = %s
                """, (batch_id,))
            conn.commit()
    finally:
        conn.close()


def get_batch_status(batch_id):
    """Возвращает текущий статус батча"""
    conn = psycopg2.connect(os.environ['SOURCE_DB_URL'])
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT status 
                FROM batch_metadata 
                WHERE batch_id = %s
            """, (batch_id,))
            return cur.fetchone()[0]
    finally:
        conn.close()


def is_retriable(status):
    """Проверяет, можно ли ретраить батч"""
    return status in ('failed', 'pending', 'in_progress')


if __name__ == '__main__':
    logger.info("Starting retry process for non-completed batches...")

    batches = get_retriable_batches()
    if not batches:
        logger.info("No retriable batches found")
        exit(0)

    logger.info(f"Found {len(batches)} retriable batches")
    retry_batches(batches)

    logger.info("Retry process finished. Check worker logs.")