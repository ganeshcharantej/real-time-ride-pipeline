# Real-Time Ride-Sharing Data Pipeline 🚖

A complete, real-time data streaming pipeline built from scratch using **Apache Kafka**, **PySpark**, and **Docker**. This project simulates a live ride-sharing application, processes the data stream in real-time, and applies the **Medallion Architecture** (Bronze, Silver, Gold) to generate a live revenue leaderboard by city.

## 🛠️ Tech Stack
* **Language:** Python
* **Message Broker:** Apache Kafka / Zookeeper
* **Stream Processing:** Apache Spark (PySpark Structured Streaming)
* **Storage Format:** Apache Parquet
* **Infrastructure:** Docker & Docker Compose

## 🏗️ Architecture: The Medallion Approach

This pipeline routes data through three distinct processing layers to progressively clean, conform, and aggregate the stream.

1. **Data Generator (The Source):** A custom Python script (`producer.py`) continuously generates synthetic ride events (ride ID, driver ID, fare, pickup location, timestamp) and publishes them to a Kafka topic.
2. **Bronze Layer (Raw):** `spark_processor.py` consumes the live Kafka stream and writes the raw JSON payloads directly to local storage as Parquet files. This ensures zero data loss and acts as the historical source of truth.
3. **Silver Layer (Cleaned):** `silver_processor.py` reads the Bronze data stream, enforces a strict `StructType` schema, casts string timestamps into native Spark `TimestampType`, filters out invalid rows, and writes the clean data to the Silver zone.
4. **Gold Layer (Aggregated):** `gold_processor.py` performs stateful streaming aggregations on the Silver data. It continuously calculates total revenue, total rides, and average fare grouped by `pickup_location`, outputting a live leaderboard to the console.

## 🚀 How to Run Locally

### 1. Start the Infrastructure
Make sure Docker Desktop is running, then spin up the Kafka and Spark cluster:
```bash
docker-compose up -d
