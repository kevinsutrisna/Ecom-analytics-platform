# End-to-End E-commerce Analytics Platform

## Project Overview
This project simulates a production-grade e-commerce analytics system. It ingests user events in real-time, stores raw data in a data lake, processes it using batch jobs, and exposes analytics-ready tables with automated orchestration and data quality checks.

## Architecture Overview
The pipeline follows a modern data engineering stack, ensuring scalability and separation of concerns:



* **Kafka**: Real-time event ingestion (keyed by `user_id`).
* **MinIO (S3-Compatible)**: Raw event storage acting as the Data Lake.
* **Spark**: Batch aggregation and transformation engine.
* **Postgres**: Serving layer for analytics-ready tables.
* **Airflow**: Workflow orchestration and scheduling.
* **Docker Compose**: Provides a zero-cost, reproducible local environment.

---

## CI/CD

This project uses **GitHub Actions** to automatically validate the data platform on every push and pull request.  
The CI pipeline builds the Docker services, starts core components, and runs smoke tests to ensure the system is functional.

---

## Key Engineering Decisions
* **Scalability**: Events are partitioned and keyed to allow horizontal scaling.
* **Data Tiering**: Clear separation between **Raw** (Bronze) and **Processed** (Gold) layers.
* **Idempotency**: Batch jobs are designed to be re-runnable without duplicating data or causing inconsistencies.
* **Production-Ready Patterns**: The infrastructure setup mirrors real-world production environments rather than taking shortcuts.

---

## Example Outputs
The system transforms raw JSON events into structured insights, such as:
* **Daily Click Count**: Aggregated per product.
* **Financial Metrics**: Daily order volume and total revenue.
* **BI-Ready Tables**: Clean, typed, and indexed tables ready for visualization tools.

---

## How to Run Locally

### 1. Prerequisites
Ensure you have **Docker** and **Docker Compose** installed on your machine.

### 2. Start Services
Clone the repository and run the following command to spin up the entire stack:
```bash
docker compose up -d
```
### 3. Access Interfaces
Once the containers are healthy, you can access the following dashboards:

| Service | URL | Default Credentials |
| :--- | :--- | :--- |
| **Airflow UI** | [http://localhost:8003](http://localhost:8003) | `airflow` / `airflow` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **Postgres (Adminer)** | [http://localhost:8082](http://localhost:8082) | *(See .env config)* |
