# Data Engineering Beehiiv-realtime

## Overview
This repository provides a comprehensive setup for a modern data engineering stack utilizing powerful open-source tools. The stack enables real-time and batch data processing, orchestration, and storage with seamless containerized deployment.

## Technologies Used

### 1. Apache Airflow
- Workflow orchestration and scheduling.
- Manages end-to-end data pipelines.
- DAG-based execution for automation.

### 2. Python
- Primary programming language for data processing and orchestration.
- Used in ETL scripts, Kafka consumers, and data transformation tasks.

### 3. Apache Kafka
- Distributed event streaming platform for real-time data ingestion.
- Handles high-throughput and low-latency data streams.
- Integrates seamlessly with Spark and ClickHouse.

### 4. Apache Zookeeper
- Manages and coordinates Kafka brokers.
- Provides leader election and distributed synchronization.

### 5. Apache Spark
- Distributed data processing engine for real-time and batch workloads.
- Utilized for transformations, aggregations, and analytics.

### 6. ClickHouse
- Columnar database optimized for fast analytical queries.
- Stores structured and semi-structured data efficiently.

### 7. PostgreSQL
- Relational database used for transactional workloads.
- Acts as metadata storage for Airflow and other applications.

### 8. Docker
- Containerization for seamless deployment of all components.
- Ensures portability and reproducibility of the data stack.

## Setup & Deployment

### Prerequisites
Ensure you have the following installed:
- Docker & Docker Compose
- Python 3.x
- Kafka & Zookeeper dependencies

### Steps to Run
1. Clone this repository:
   ```sh
   git clone https://github.com/Theglassofdata/Beehiiv-realtime.git
   cd Beehiiv-realtime
   ```
2. Start the services using Docker Compose:
   ```sh
   docker-compose up -d
   ```
3. Verify Airflow setup:
   ```sh
   docker-compose exec airflow-webserver airflow dags list
   ```
4. Access services:
   - **Airflow UI:** http://localhost:8080
   - **Kafka UI (if available):** http://localhost:9092
   - **ClickHouse:** Connect via `clickhouse-client`
   - **PostgreSQL:** Access using `psql` or admin tools

## Contributing
Feel free to open issues and contribute improvements to this stack.

## License
This project is licensed under the MIT License.
