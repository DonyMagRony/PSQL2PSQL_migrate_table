# README: Data Migration with Celery and PostgreSQL

## **Project Overview**
This project uses **Celery** to process batches of data from a **source PostgreSQL database** to a **destination PostgreSQL database** asynchronously.
It utilizes **Redis** as a message broker and supports **parallel processing** with multiple worker instances.

## **Setup and Running the Project**

###  Build and Start the Containers**
Run the following command to build the Docker images:
```sh
docker-compose build
```
Then, start the services:
```sh
docker-compose up -d
```
This will start:
- `source_db` (PostgreSQL)
- `dest_db` (PostgreSQL)
- `redis` (Message Broker)
- `worker` (Celery worker instances)
- `scheduler` (Task scheduler for processing batches)
- `validation` (validation)

###  Alternative Way to Start (PyCharm Services)**
If using **PyCharm**, navigate to **Services** and start `scheduler` and `worker` containers manually.

#### **Manually Retry a Failed Batch**
docker exec -it pythonproject-worker-1 python retry_retriable.py
