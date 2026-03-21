# 🚀 Medallion Data Pipeline: Airflow, Spark & Apache Iceberg

A production-grade data engineering project implementing the **Medallion Architecture (Bronze, Silver, Gold)** using:

- **Apache Airflow** for orchestration  
- **PySpark** for distributed processing  
- **Apache Iceberg** for ACID-compliant table management  
- **MinIO (S3-compatible)** for object storage  

## 📖 Article

👉 [Building a Scalable Medallion Pipeline: Airflow, Spark, and Apache Iceberg](https://medium.com/@hufs0529/building-a-scalable-medallion-pipeline-airflow-spark-and-apache-iceberg-328dd05c68bd)

---


## 🏗 System Architecture

This project follows the **Medallion (Multi-Hop) Architecture**, ensuring data quality as it flows from raw ingestion to business-ready insights.

### 🥉 Bronze (Raw)
- Ingest raw CSV data from local storage  
- Upload to MinIO buckets using `boto3`  
- No transformations (raw fidelity preserved)

---

### 🥈 Silver (Refined)
- Convert raw data → **Iceberg tables**
- Perform:
  - Deduplication
  - Data normalization
  - Table joins
- Use **Iceberg `MERGE INTO`** for efficient UPSERT

```sql
MERGE INTO my_catalog.silver.orders_dupl AS target
USING batch AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### 🥇 Gold (Curated)

- Build **Star Schema** for analytics  
- Optimized for BI tools & dashboards  

#### Tables

- `fact_sales` → central fact table  
- `dim_product`, `dim_customer` → dimension tables  
- `daily_sales` → pre-aggregated metrics  

---

## 🐳 Infrastructure as Code (IaC)

### Custom Docker Environment

To eliminate **dependency hell**, the entire system is containerized.

#### Dockerfile includes

- Java Runtime (JRE) → required for Spark  
- Pre-installed JARs:
  - Apache Iceberg runtime  
  - Hadoop AWS connectors  
- Global classpath:
  - `SPARK_DIST_CLASSPATH`  

---

## ⚙️ Spark Runner Utility

A centralized `SparkJobRunner` class abstracts Spark configuration.

### Responsibilities

- SparkSession creation  
- Iceberg catalog configuration (Hadoop-based)  
- MinIO (S3A) connection setup:
  - Access keys  
  - Endpoint config  
- Enable ACID transactions via Spark SQL extensions  
- Logging & error handling  

---

## 📂 Project Structure

```plaintext
.
├── dags/
│   ├── pipelines/
│   │   └── medallion_pipeline.py     # Main Airflow DAG
│   ├── spark_jobs/
│   │   ├── bronze/                  # Raw ingestion jobs
│   │   ├── silver/                  # Deduplication, merging, joins
│   │   └── gold/                    # Star schema & aggregations
│   └── utils/
│       ├── spark_runner.py          # SparkSession factory
│       └── load_raw_data.py         # MinIO ingestion logic
├── Dockerfile                      # Custom Airflow + Spark image
└── docker-compose.yaml             # Airflow, Postgres, MinIO setup

## 🚀 Key Data Logic

### 1. Advanced Deduplication (Silver Layer)

- Uses **Iceberg MERGE INTO**  
- Supports **UPSERT without full overwrite**  
- Efficient for incremental pipelines  

---

### 2. Star Schema Modeling (Gold Layer)

#### Fact Table

- `fact_sales`
  - Joins:
    - payments  
    - reviews  
    - order items  

#### Dimension Tables

- `dim_product`  
- `dim_customer`  

#### Aggregations

- `daily_sales`  
  - Precomputed metrics for dashboards  

---

## 🔧 Getting Started

### Prerequisites

- Docker  
- Docker Compose  
- Olist E-commerce Dataset (placed in `raw-data/` directory)  

---

## ▶️ Execution

### 1. Build Docker Image

```bash
docker-compose build
```

### 2. Start Infrastructure
```bash
docker-compose up -d
```

### 3. Access Airflow
```bash
http://localhost:8080
```

### 4. Run Pipeline
```bash
medallion_v2_spark_submit
```