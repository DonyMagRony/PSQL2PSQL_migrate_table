FROM python:3.9
RUN pip install celery redis sqlalchemy psycopg2-binary
COPY worker.py scheduler.py /app/
WORKDIR /app