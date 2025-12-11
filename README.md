# LakeHouse Travel - Tourism Data Analytics Platform

<div align="center">

![Architecture](https://img.shields.io/badge/Architecture-Lakehouse-blue)
![Data Processing](https://img.shields.io/badge/Processing-Apache%20Spark-orange)
![Orchestration](https://img.shields.io/badge/Orchestration-Apache%20Airflow-green)
![Storage](https://img.shields.io/badge/Storage-Apache%20Iceberg-purple)
![ML](https://img.shields.io/badge/ML-XGBoost%20%2B%20MLflow-red)
![Analytics](https://img.shields.io/badge/Analytics-Dremio-cyan)

</div>

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Data Pipeline](#data-pipeline)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Installation & Setup](#installation--setup)
- [Usage](#usage)
- [Data Sources](#data-sources)
- [Machine Learning](#machine-learning)
- [Web Interface](#web-interface)
- [Development](#development)
- [Troubleshooting](#troubleshooting)
- [License](#license)

---

## Overview

**LakeHouse Travel** là một nền tảng phân tích dữ liệu du lịch toàn diện, được xây dựng theo kiến trúc **Lakehouse** hiện đại. Dự án tập trung vào việc thu thập, xử lý và phân tích dữ liệu từ nhiều nguồn (Booking.com, TikTok) để cung cấp insights về xu hướng du lịch và dự báo mức độ "hot" của các tỉnh thành Việt Nam.

### Key Features

✅ **Modern Lakehouse Architecture** - Kết hợp ưu điểm của Data Lake và Data Warehouse  
✅ **Multi-Source Data Integration** - Thu thập từ Booking.com, TikTok  
✅ **Automated ETL Pipeline** - Xử lý tự động với Apache Airflow  
✅ **Big Data Processing** - Apache Spark với Apache Iceberg  
✅ **ML-Based Forecasting** - Dự báo xu hướng du lịch với XGBoost  
✅ **Interactive Analytics** - Dremio Query Engine + Gradio Web UI  
✅ **MLOps Integration** - MLflow cho experiment tracking và model registry  

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │ Booking.com  │  │   TikTok     │  │  Province    │          │
│  │   Hotels     │  │   Videos     │  │     Data     │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER                               │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │              Apache Airflow (Orchestrator)               │   │
│  │  • Bronze DAG  • Silver DAG  • Gold DAG  • ML DAG       │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                   PROCESSING LAYER                               │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │      Apache Spark Cluster (Master + Workers)            │   │
│  │  • PySpark Jobs  • Data Transformation  • ML Training   │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                     STORAGE LAYER                                │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │    MinIO     │  │   Hive       │  │  PostgreSQL  │          │
│  │  (S3 Store)  │  │  Metastore   │  │  (Metadata)  │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
│  ┌───────────────────────────────────────────────────┐          │
│  │         Apache Iceberg Tables                     │          │
│  │  Bronze → Silver → Gold (Medallion Architecture) │          │
│  └───────────────────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                  ANALYTICS & ML LAYER                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │   Dremio     │  │    MLflow    │  │   Gradio     │          │
│  │ (Query Eng)  │  │ (ML Tracking)│  │  (Web App)   │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
```

### Medallion Architecture

Dự án tuân theo kiến trúc **Medallion** (Bronze → Silver → Gold):

- **🥉 Bronze Layer**: Dữ liệu thô (raw) từ các nguồn, lưu dưới dạng Parquet
- **🥈 Silver Layer**: Dữ liệu đã được làm sạch, chuẩn hóa và validate
- **🥇 Gold Layer**: Dữ liệu đã được aggregate, có sẵn dimensions và fact tables cho analytics

---

## Technology Stack

### Core Infrastructure

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Container Platform** | Docker & Docker Compose | - | Container orchestration |
| **Workflow Orchestration** | Apache Airflow | 2.10.2 | ETL pipeline scheduling |
| **Distributed Processing** | Apache Spark | 3.5.0 | Big data processing |
| **Table Format** | Apache Iceberg | 1.4.3 | ACID transactions, time travel |
| **Object Storage** | MinIO | Latest | S3-compatible storage |
| **Metadata Store** | Hive Metastore | 3.1.3 | Table metadata management |
| **Database** | PostgreSQL | 15 | Metadata & MLflow backend |

### Analytics & ML

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| **Query Engine** | Dremio | Latest | Interactive SQL analytics |
| **ML Framework** | XGBoost | - | Time-series forecasting |
| **ML Ops** | MLflow | - | Experiment tracking, model registry |
| **Web Framework** | Gradio | - | Interactive web application |
| **NLP Library** | Underthesea | - | Vietnamese text processing |

### Programming Languages

- **Python 3.11** - Primary language cho Spark jobs, Airflow DAGs, ML
- **SQL** - Data queries và transformations
- **Bash** - Deployment scripts

---

## Data Pipeline

### Bronze Layer (Raw Ingestion)

**DAG**: `bronze_raw_ingestion`  
**Schedule**: Manual trigger  
**Purpose**: Thu thập dữ liệu thô từ CSV files và lưu vào MinIO

**Datasets**:
- `vietnam_hotels_list.csv` - Danh sách khách sạn (13 tỉnh)
- `vietnam_hotels_detail.csv` - Chi tiết khách sạn
- `vietnam_hotels_reviews.csv` - Reviews (1M+ records)
- `tiktok_videos.csv` - TikTok videos về du lịch
- `tiktok_comments_*.csv` - Comments từ TikTok (6 batch files)

**Key Features**:
- File checksum tracking (tránh duplicate)
- UTF-8 encoding preservation
- Parallel ingestion cho datasets độc lập
- Logging vào PostgreSQL `file_ingestion_log` table

### Silver Layer (Data Transformation)

**DAG**: `silver_transformation`  
**Schedule**: Triggered sau Bronze layer  
**Purpose**: Làm sạch, validate và standardize data

**Transformations**:

1. **Hotels Detail**
   - Clean province names (loại bỏ ký tự đặc biệt cho S3 partition)
   - Extract latitude/longitude từ text
   - Parse JSON arrays (facilities, room_types)
   - Validate NOT NULL constraints

2. **Hotels List**
   - Standardize coordinates
   - Clean address fields
   - Map regions (Northeast, Southeast, v.v.)

3. **Hotels Reviews**
   - Parse review dates
   - Extract traveler types (Solo, Couple, Family, Business)
   - Clean review text
   - Calculate sentiment scores

4. **TikTok Videos**
   - Parse timestamps
   - Extract author info
   - Clean video descriptions
   - Map to provinces via destination matching

5. **TikTok Comments**
   - **NLP Processing** với Underthesea:
     - Sentiment analysis (positive/negative)
     - Word count & unique word ratio
     - Emoji detection (positive/negative emojis)
   - Parent-child comment hierarchy
   - Deduplication

**Resource Management**:
- **Phase 1** (Parallel): Light jobs (hotels_detail, hotels_list, tiktok_videos)
- **Phase 2** (Sequential): Heavy jobs (hotels_reviews → tiktok_comments)
- Executor memory: 2GB (light) → 4GB (heavy)

### Gold Layer (Analytics Ready)

**DAG**: `gold_aggregation`  
**Schedule**: Triggered sau Silver layer  
**Purpose**: Tạo star schema với dimensions và fact tables

**Dimension Tables**:
- `dim_date` - Date dimension (2015-2025)
- `dim_province` - 63 tỉnh/thành Việt Nam với region mapping
- `dim_destination` - Điểm đến du lịch
- `dim_author` - TikTok authors
- `dim_post` - TikTok videos/posts
- `dim_comment` - TikTok comments với NLP metrics
- `dim_hotel` - Hotels với location info
- `dim_room_type` - Room types (Suite, Deluxe, v.v.)
- `dim_travel_type` - Travel types (Solo, Couple, v.v.)
- `dim_country` - Countries (reviewer origins)

**Fact Tables**:
- `fact_province_content_engagement` - TikTok engagement metrics by province
- `fact_hotel_review` - Hotel review analytics
- `fact_comment_nlp` - NLP analytics cho comments

**Key Features**:
- Surrogate keys (SK) cho tất cả dimensions
- Foreign key relationships
- Partition by province_sk cho performance
- ACID transactions với Iceberg MERGE operations

### ML Layer (Forecasting)

**Job**: `train_and_forecast.py`  
**Schedule**: Triggered sau Gold layer aggregation  
**Purpose**: Train XGBoost model để dự báo province hotness

**Pipeline**:

1. **Feature Engineering**
   - Calculate `hotness_score` từ 11 metrics:
     - Volume: total_comments, total_posts
     - Post Engagement: total_post_likes, total_post_saves
     - Sentiment: positive_ratio, avg_sentiment_score
     - NLP Richness: avg_words_per_comment, avg_unique_word_ratio, total_positive_emojis, total_emojis, total_negative_emojis
   
   - Temporal features:
     - Month (1-12)
     - Month sin/cos (cyclical encoding)
   
   - Lag features:
     - hotness_lag_1, lag_2, lag_3, lag_12 (1, 2, 3, 12 tháng trước)
     - hotness_rolling_avg_3m (rolling average 3 tháng)

2. **Model Training**
   - Algorithm: XGBoost Regressor
   - Train/Test Split: 70/30
   - Cross-validation với time series split
   - Hyperparameters:
     ```python
     {
       "n_estimators": 200,
       "max_depth": 4,
       "learning_rate": 0.01,
       "min_child_weight": 5,
       "subsample": 0.7,
       "colsample_bytree": 0.7,
       "gamma": 0.1,
       "reg_alpha": 0.1,
       "reg_lambda": 1.0
     }
     ```

3. **MLflow Tracking**
   - Experiment name: `province_hotness_forecasting`
   - Logged metrics: RMSE, MAE, R², MAPE
   - Artifacts: Feature importance plots, actual vs predicted charts
   - Model registry: `province_hotness_forecaster`

4. **Forecasting**
   - Recursive autoregressive forecasting (12 tháng)
   - Output table: `gold.gold.province_month_forecast_next12`
   - Columns: province_sk, province_name, forecast_month, predicted_hotness, prediction_date

---

## Project Structure

```
LakeHousePj/
├── docker-compose.yml                 # Main orchestration file
├── .env                               # Environment variables
├── .gitignore                         # Git ignore rules
│
├── airflow/                           # Airflow configuration
│   ├── Dockerfile                     # Custom Airflow image
│   ├── requirements.txt               # Python dependencies
│   └── dags/                          # DAG definitions
│       ├── bronze/                    # Bronze layer DAGs
│       │   ├── dag.py                 # Raw ingestion DAG
│       │   └── config.py              # Bronze config
│       ├── silver/                    # Silver layer DAGs
│       │   ├── dag.py                 # Transformation DAG
│       │   └── config.py              # Silver config
│       ├── gold/                      # Gold layer DAGs
│       │   ├── dag.py                 # Aggregation DAG
│       │   └── config.py              # Gold config
│       └── common/                    # Shared utilities
│           ├── spark_operators.py     # Spark submit helpers
│           ├── health_checks.py       # Container health checks
│           └── notifications.py       # Logging utilities
│
├── spark/                             # Spark configuration
│   ├── Dockerfile                     # Spark cluster image
│   ├── spark-defaults.conf            # Spark config
│   └── jobs/                          # PySpark jobs
│       ├── bronze/                    # Bronze ingestion jobs
│       │   ├── raw_ingest_booking_hotels_list.py
│       │   ├── raw_ingest_booking_hotels_detail.py
│       │   ├── raw_ingest_booking_hotels_reviews.py
│       │   ├── raw_ingest_tiktok_videos.py
│       │   └── raw_ingest_tiktok_comments_batch.py
│       ├── silver/                    # Silver transformation jobs
│       │   ├── hotels_detail/
│       │   │   ├── step_01_transform.py
│       │   │   ├── step_02_clean_load.py
│       │   │   └── config.py
│       │   ├── hotels_list/
│       │   ├── hotels_reviews/
│       │   ├── tiktok_videos/
│       │   └── tiktok_comments/
│       ├── gold/                      # Gold aggregation jobs
│       │   ├── dim_date/
│       │   ├── dim_province/
│       │   ├── dim_destination/
│       │   ├── dim_author/
│       │   ├── dim_post/
│       │   ├── dim_comment/
│       │   ├── dim_hotel/
│       │   ├── dim_room_type/
│       │   ├── dim_travel_type/
│       │   ├── dim_country/
│       │   ├── fact_province_content_engagement/
│       │   ├── fact_hotel_review/
│       │   └── TrainingModel/
│       │       └── train_province_model.py
│       ├── ml/                        # ML pipeline
│       │   └── train_and_forecast.py  # XGBoost forecasting
│       └── utils/                     # Shared utilities
│           ├── spark_session.py       # Spark session factory
│           ├── iceberg_utils.py       # Iceberg helpers
│           ├── file_tracker.py        # File ingestion tracking
│           ├── s3_utf8_uploader.py    # UTF-8 S3 uploader
│           ├── merge_utils.py         # MERGE operation helpers
│           └── gold_job_logger.py     # Gold job logging
│
├── dremio/                            # Dremio query engine
│   ├── Dockerfile
│   ├── SETUP.md                       # Dremio setup guide
│   └── conf/
│       └── dremio.conf
│
├── gradio/                            # Web application
│   ├── Dockerfile
│   ├── app_forecast.py                # Gradio app
│   └── requirements.txt
│
├── mlflow/                            # MLflow server
│   ├── Dockerfile
│   └── data/                          # MLflow backend data
│
├── postgres/                          # PostgreSQL database
│   ├── data/                          # Database files
│   └── init/                          # Initialization scripts
│       ├── init.sql
│       ├── 02_create_file_tracking.sql
│       ├── 03_create_date_db.sql
│       └── 04_create_mlflow_db.sql
│
├── hive/                              # Hive Metastore
│   ├── Dockerfile
│   ├── init/
│   │   └── hive-site.xml
│   └── scripts/
│       └── check-hive-db.ps1
│
├── minio/                             # MinIO object storage
│   ├── config/
│   └── data/                          # S3 buckets (bronze/silver/gold)
│
├── pgadmin/                           # pgAdmin web UI
│   ├── servers.json                   # Pre-configured servers
│   └── data/
│
└── data/                              # Source data
    ├── raw/
    │   ├── booking/                   # Booking.com CSVs
    │   └── tiktok/                    # TikTok CSVs
    ├── VietNam_Province/              # Province reference data
    └── DateDimension/                 # Date dimension SQL
```

---

## Prerequisites

### System Requirements

- **OS**: Windows 10/11, macOS, hoặc Linux
- **RAM**: Tối thiểu 16GB (khuyến nghị 32GB)
- **CPU**: 4+ cores (khuyến nghị 8+ cores)
- **Disk**: Tối thiểu 50GB free space
- **Docker**: Desktop 4.x hoặc cao hơn
- **Docker Compose**: v2.x

### Software Dependencies

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Git](https://git-scm.com/)
- PowerShell (Windows) hoặc Bash (Linux/macOS)

---

## Installation & Setup

### 1. Clone Repository

```bash
git clone <repository-url>
cd LakeHousePj
```

### 2. Configure Environment

Copy file `.env` và điều chỉnh nếu cần:

```bash
# File .env đã có sẵn với cấu hình mặc định
# Kiểm tra và thay đổi passwords nếu cần thiết
```

**Default Credentials**:
- **PostgreSQL**: `lakehouse_user` / `lakehouse_pass`
- **MinIO**: `minioadmin` / `minioadmin123`
- **Airflow**: `admin` / `admin`
- **pgAdmin**: `admin@admin.com` / `admin123`

### 3. Prepare Data Files

Đặt các file CSV vào thư mục `data/raw/`:

```
data/raw/
├── booking/
│   ├── vietnam_hotels_list.csv
│   ├── vietnam_hotels_detail.csv
│   └── vietnam_hotels_reviews.csv
└── tiktok/
    ├── links/
    │   └── *.csv (TikTok videos)
    └── comments/
        └── *.csv (TikTok comments)
```

### 4. Start Infrastructure

```bash
# Start all services
docker-compose up -d

# Check logs
docker-compose logs -f

# Verify services are healthy
docker-compose ps
```

**Startup Time**: Khoảng 2-3 phút cho tất cả services

### 5. Verify Services

Sau khi khởi động, truy cập các web interfaces:

| Service | URL | Purpose |
|---------|-----|---------|
| **Airflow** | http://localhost:8082 | Workflow orchestration |
| **Spark Master** | http://localhost:8080 | Spark cluster monitoring |
| **MinIO Console** | http://localhost:9001 | S3 storage management |
| **Dremio** | http://localhost:9047 | Query engine |
| **MLflow** | http://localhost:5001 | ML experiment tracking |
| **Gradio** | http://localhost:7860 | Forecasting web app |
| **pgAdmin** | http://localhost:5050 | Database management |

### 6. Initialize Dremio (First Time Only)

Truy cập http://localhost:9047 và setup:

1. Tạo admin account
2. Add source:
   - **Type**: Hive
   - **Name**: `hive_metastore`
   - **Host**: `hive-metastore`
   - **Port**: `9083`

Xem chi tiết: `dremio/SETUP.md`

---

## Usage

### Running ETL Pipeline

#### Option 1: Full Pipeline (Recommended)

1. Truy cập Airflow UI: http://localhost:8082
2. Login: `admin` / `admin`
3. Enable và trigger DAGs theo thứ tự:
   - ✅ `bronze_raw_ingestion` → Chờ complete
   - ✅ `silver_transformation` → Chờ complete
   - ✅ `gold_aggregation` → Chờ complete

#### Option 2: Individual DAG

```bash
# Trigger Bronze DAG
docker exec lakehouse_airflow airflow dags trigger bronze_raw_ingestion

# Check DAG status
docker exec lakehouse_airflow airflow dags list

# View logs
docker exec lakehouse_airflow airflow tasks logs bronze_raw_ingestion <task_id> <execution_date>
```

### Running ML Training & Forecasting

Sau khi Gold layer hoàn thành:

```bash
# Trigger ML job từ Spark
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --driver-memory 2g \
  --executor-memory 2g \
  --executor-cores 2 \
  /opt/spark/jobs/ml/train_and_forecast.py
```

Hoặc trigger từ Airflow (nếu đã thêm ML DAG).

### Viewing Forecast Results

#### Option 1: Gradio Web App (Recommended)

1. Truy cập http://localhost:7860
2. Select:
   - **Start Month**: Tháng bắt đầu dự báo
   - **Forecast Months**: Số tháng dự báo (1-12)
   - **Region Filter**: Vùng miền (optional)
   - **Top N Provinces**: Số lượng tỉnh hiển thị
3. Click **"Forecast Province Hotness"**
4. Xem:
   - Line chart: Hotness trend theo thời gian
   - Table: Top provinces với predicted hotness scores

#### Option 2: Dremio SQL Query

```sql
-- View forecast results
SELECT 
  province_name,
  forecast_month,
  predicted_hotness,
  prediction_date
FROM gold.gold.province_month_forecast_next12
ORDER BY forecast_month, predicted_hotness DESC
LIMIT 100;
```

#### Option 3: MLflow UI

1. Truy cập http://localhost:5001
2. Click experiment: `province_hotness_forecasting`
3. View:
   - Run metrics (RMSE, MAE, R²)
   - Feature importance plots
   - Actual vs Predicted charts
4. Download model từ Models tab

---

## Data Sources

### Booking.com Hotels Dataset

**Files**:
- `vietnam_hotels_list.csv` (~1,400 hotels)
- `vietnam_hotels_detail.csv` (~1,400 hotels)
- `vietnam_hotels_reviews.csv` (~1M reviews)

**Coverage**: 13 tỉnh/thành phố chính:
- Hà Nội, TP. Hồ Chí Minh, Đà Nẵng, Hội An, Huế
- Nha Trang, Đà Lạt, Phú Quốc, Vũng Tàu
- Hạ Long, Sapa, Phan Thiết, Cần Thơ

**Fields**:
- Hotel info: name, address, coordinates, facilities, room types
- Reviews: text, rating, date, reviewer country, travel type

### TikTok Tourism Content Dataset

**Files**:
- `tiktok_videos.csv` (~10,000 videos)
- `tiktok_comments_*.csv` (6 batches, ~500K comments)

**Coverage**: Videos về địa điểm du lịch Việt Nam

**Fields**:
- Video: URL, description, likes, shares, saves, comments count
- Author: username, follower count
- Comments: text, likes, reply count, sentiment, NLP features

### Province Reference Data

**Files**:
- `List_Destination.csv` - Mapping destinations → provinces
- `diaphantinh.json` - Province boundaries (GeoJSON)

**Data**: 63 tỉnh/thành với region classification

---

## Machine Learning

### Hotness Score Calculation

**Formula**:
```python
hotness_score = (
    # Volume (30%)
    0.15 * total_comments_normalized +
    0.15 * total_posts_normalized +
    
    # Post Engagement (25%)
    0.15 * total_post_likes_normalized +
    0.10 * total_post_saves_normalized +
    
    # Sentiment (20%)
    0.10 * positive_ratio +
    0.10 * avg_sentiment_score +
    
    # NLP Richness (25%)
    0.05 * avg_words_per_comment_normalized +
    0.05 * avg_unique_word_ratio +
    0.05 * total_positive_emojis_normalized +
    0.05 * total_emojis_normalized +
    0.05 * total_negative_emojis_normalized
) * 100
```

**Normalization**: Min-Max scaling (0-1) cho mỗi metric

### Model Performance

**Typical Metrics** (sẽ thay đổi tùy data):
- **RMSE**: ~2.5-3.5
- **MAE**: ~1.8-2.5
- **R²**: ~0.75-0.85
- **MAPE**: ~15-25%

**Feature Importance** (top 5):
1. `hotness_lag_1` (most recent hotness)
2. `hotness_rolling_avg_3m`
3. `total_comments`
4. `month_sin` / `month_cos` (seasonality)
5. `positive_ratio`

### Model Registry

- **Model Name**: `province_hotness_forecaster`
- **Backend**: MLflow Registry
- **Storage**: PostgreSQL (metadata) + MinIO (artifacts)
- **Versioning**: Automatic với run_id

---

## Web Interface

### Gradio App (`app_forecast.py`)

**Features**:
- 📅 **Flexible Time Range**: Chọn start month và forecast horizon (1-12 tháng)
- 🗺️ **Region Filter**: Filter theo 8 vùng miền Việt Nam
- 📊 **Interactive Charts**: Plotly line charts với zoom/pan
- 📋 **Data Table**: Sortable forecast results
- 🔄 **Auto Refresh**: Cache 30 giây, tự động load model mới

**Data Flow**:
```
MinIO (gold/ml_forecast/*.parquet)
    ↓
Load latest run folder
    ↓
Filter by region + date range
    ↓
Join with province data
    ↓
Display chart + table
```

**Regions**:
1. Đông Bắc (Northeast)
2. Tây Bắc (Northwest)
3. Đồng bằng Sông Hồng (Red River Delta)
4. Bắc Trung Bộ (North Central Coast)
5. Tây Nguyên (Central Highlands)
6. Duyên hải Nam Trung Bộ (South Central Coast)
7. Đông Nam Bộ (Southeast)
8. Đồng bằng Sông Cửu Long (Mekong Delta)

---

## Development

### Adding New Data Source

1. **Bronze Layer**:
   - Thêm ingest job: `spark/jobs/bronze/raw_ingest_<source>.py`
   - Update `airflow/dags/bronze/config.py`
   - Thêm task vào `bronze/dag.py`

2. **Silver Layer**:
   - Tạo transformation job: `spark/jobs/silver/<source>/step_01_transform.py`
   - Tạo clean & load job: `spark/jobs/silver/<source>/step_02_clean_load.py`
   - Update `silver/config.py`

3. **Gold Layer**:
   - Design dimension/fact tables
   - Implement job: `spark/jobs/gold/<table_name>/<table_name>_job.py`
   - Update dependencies trong `gold/dag.py`

### Running Tests

```bash
# Test Spark job locally
docker exec lakehouse_spark_master /opt/spark/bin/spark-submit \
  --master local[*] \
  /opt/spark/jobs/<path_to_job>.py

# Check Iceberg table
docker exec lakehouse_spark_master /opt/spark/bin/spark-sql \
  --conf spark.sql.defaultCatalog=gold \
  -e "SELECT * FROM gold.gold.<table_name> LIMIT 10;"
```

### Database Schema Inspection

```bash
# Connect to PostgreSQL
docker exec -it lakehouse_postgres psql -U lakehouse_user -d metastore_db

# List Iceberg tables
\dt

# Describe table structure
\d+ file_ingestion_log
```

### MinIO Data Inspection

```bash
# List buckets
docker exec lakehouse_minio mc ls myminio

# List files in bucket
docker exec lakehouse_minio mc ls myminio/bronze/
docker exec lakehouse_minio mc ls myminio/silver/
docker exec lakehouse_minio mc ls myminio/gold/

# Download file
docker exec lakehouse_minio mc cp myminio/gold/ml_forecast/<file>.parquet /tmp/
```

---

## Troubleshooting

### Common Issues

#### 1. Containers Not Starting

**Symptoms**: `docker-compose ps` shows "Unhealthy" or "Exited"

**Solutions**:
```bash
# Check logs
docker-compose logs <service_name>

# Restart specific service
docker-compose restart <service_name>

# Rebuild image
docker-compose up -d --build <service_name>

# Clean restart
docker-compose down -v
docker-compose up -d
```

#### 2. Airflow DAG Not Visible

**Symptoms**: DAG không xuất hiện trong Airflow UI

**Solutions**:
```bash
# Check DAG file syntax
docker exec lakehouse_airflow python /opt/airflow/dags/bronze/dag.py

# Refresh DAGs
docker exec lakehouse_airflow airflow dags list-import-errors

# Restart scheduler
docker-compose restart airflow
```

#### 3. Spark Job Fails with OOM

**Symptoms**: `java.lang.OutOfMemoryError` trong Spark logs

**Solutions**:
```yaml
# Tăng executor memory trong spark-defaults.conf
spark.executor.memory=4g
spark.driver.memory=2g

# Hoặc điều chỉnh trong spark-submit command
--executor-memory 4g --driver-memory 2g
```

#### 4. Hive Metastore Connection Refused

**Symptoms**: `Connection refused` khi Spark kết nối Hive

**Solutions**:
```bash
# Check Hive Metastore status
docker exec lakehouse-hive-metastore netcat -zv localhost 9083

# Restart Hive Metastore
docker-compose restart hive-metastore

# Verify Hive database exists
docker exec lakehouse_postgres psql -U lakehouse_user -d metastore_db -c "\dt"
```

#### 5. MinIO Buckets Not Created

**Symptoms**: S3 errors như "NoSuchBucket"

**Solutions**:
```bash
# Check minio-init logs
docker-compose logs minio-init

# Manually create buckets
docker exec lakehouse_minio mc alias set myminio http://minio:9000 minioadmin minioadmin123
docker exec lakehouse_minio mc mb myminio/bronze
docker exec lakehouse_minio mc mb myminio/silver
docker exec lakehouse_minio mc mb myminio/gold
```

#### 6. Dremio Cannot Connect to Hive

**Symptoms**: "Failed to connect" trong Dremio source setup

**Solutions**:
- Kiểm tra Hive Metastore đang chạy: `docker ps | grep hive`
- Verify hostname resolution: `docker exec lakehouse-dremio ping hive-metastore`
- Check Dremio config: `dremio/conf/dremio.conf`
- Restart Dremio: `docker-compose restart dremio`

#### 7. MLflow Tracking Not Working

**Symptoms**: Experiments không save vào MLflow

**Solutions**:
```bash
# Check MLflow database exists
docker exec lakehouse_postgres psql -U lakehouse_user -l | grep mlflow_db

# Verify environment variable
docker exec lakehouse_spark_master printenv | grep MLFLOW

# Test connection
docker exec lakehouse_spark_master python3 -c "
import mlflow
mlflow.set_tracking_uri('postgresql://lakehouse_user:lakehouse_pass@postgres:5432/mlflow_db')
print(mlflow.list_experiments())
"
```

#### 8. Gradio App Shows No Data

**Symptoms**: Empty charts trong Gradio web app

**Solutions**:
```bash
# Check if forecast data exists
docker exec lakehouse_minio mc ls myminio/gold/ml_forecast/

# Clear cache và reload
docker-compose restart gradio

# Check Gradio logs
docker-compose logs gradio
```

### Resource Management

**Recommended Docker Settings**:
- **Memory**: 10-12GB (Preferences → Resources)
- **CPU**: 6+ cores
- **Disk**: 50GB+

**Stop Unused Services**:
```bash
# Stop non-essential services để tiết kiệm RAM
docker-compose stop pgadmin dremio gradio
```

**Clean Up Docker**:
```bash
# Remove unused containers/images
docker system prune -a

# Remove volumes (⚠️ will delete data)
docker-compose down -v
```

---

## Performance Optimization

### Spark Tuning

**Config**: `spark/spark-defaults.conf`

```properties
# Memory settings
spark.executor.memory=3g
spark.driver.memory=2g
spark.executor.cores=2

# Shuffle optimization
spark.sql.shuffle.partitions=200
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true

# S3 optimization
spark.hadoop.fs.s3a.multipart.size=104857600  # 100MB
spark.hadoop.fs.s3a.fast.upload=true
```

### Iceberg Optimization

**Table Properties**:
```sql
-- Partition pruning
ALTER TABLE gold.gold.fact_province_content_engagement 
SET TBLPROPERTIES (
  'write.parquet.compression-codec'='snappy',
  'write.target-file-size-bytes'='134217728'  -- 128MB
);

-- Compaction (compact small files)
CALL gold.system.rewrite_data_files(
  table => 'gold.gold.fact_province_content_engagement',
  options => map('target-file-size-bytes', '134217728')
);
```

### Query Optimization (Dremio)

```sql
-- Enable reflection (materialized view)
CREATE REFLECTION <reflection_name> ON gold.gold.<table_name>
USING DIMENSIONS (province_sk, date_sk)
MEASURES (SUM(likes), AVG(engagement_score));

-- Use partitioned queries
SELECT * FROM gold.gold.fact_province_content_engagement
WHERE province_sk = 1  -- Partition pruning
  AND date_sk >= 20240101;
```

---

## Monitoring & Logging

### Service Health Checks

```bash
# Check all services status
docker-compose ps

# View resource usage
docker stats

# Check specific service logs
docker-compose logs -f airflow
docker-compose logs -f spark-master
docker-compose logs -f hive-metastore
```

### Airflow Monitoring

- **Web UI**: http://localhost:8082
- **DAG Success/Failure**: Browse → DAGs → Click DAG → Graph/Calendar view
- **Task Logs**: Click task → View logs
- **Metrics**: Admin → Metrics (requires StatsD setup)

### Spark Monitoring

- **Master UI**: http://localhost:8080 (cluster status, workers)
- **Application UI**: http://localhost:4040 (jobs, stages, storage)
- **History Server**: http://localhost:18080 (completed apps)

### PostgreSQL Monitoring

```bash
# Connect to database
docker exec -it lakehouse_postgres psql -U lakehouse_user -d metastore_db

# Check file ingestion log
SELECT layer, status, COUNT(*) 
FROM file_ingestion_log 
GROUP BY layer, status;

# Check table sizes
SELECT 
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### MinIO Monitoring

- **Web Console**: http://localhost:9001
- **Metrics**: Navigate to Monitoring → Metrics
- **Usage**: View bucket sizes, object counts

---

## Security Considerations

### Production Checklist

⚠️ **Dự án này sử dụng cấu hình development. Để deploy production:**

1. **Change Default Passwords**:
   - PostgreSQL: Update `.env` → `POSTGRES_PASSWORD`
   - MinIO: Update `.env` → `MINIO_ROOT_PASSWORD`
   - Airflow: Update `docker-compose.yml` → `_AIRFLOW_WWW_USER_PASSWORD`

2. **Enable SSL/TLS**:
   - MinIO: Configure certificates cho HTTPS
   - Dremio: Enable SSL cho client connections
   - Airflow: Configure reverse proxy với SSL

3. **Network Segmentation**:
   - Remove port exposure cho internal services (Hive, Postgres)
   - Use reverse proxy (Nginx, Traefik) cho external access
   - Configure firewall rules

4. **Data Encryption**:
   - Enable encryption at rest cho MinIO buckets
   - Configure Iceberg encryption
   - Use encrypted PostgreSQL connections

5. **Access Control**:
   - Implement RBAC trong Airflow
   - Configure Dremio user roles/privileges
   - Restrict MinIO bucket policies

6. **Audit Logging**:
   - Enable audit logs cho tất cả services
   - Ship logs tới central logging system (ELK, Splunk)

---

## Backup & Recovery

### Backup Strategy

#### 1. PostgreSQL (Metadata)

```bash
# Backup all databases
docker exec lakehouse_postgres pg_dumpall -U lakehouse_user > backup_$(date +%Y%m%d).sql

# Restore
cat backup_20241211.sql | docker exec -i lakehouse_postgres psql -U lakehouse_user
```

#### 2. MinIO (Data Lake)

```bash
# Backup using MinIO client
docker exec lakehouse_minio mc mirror myminio/bronze/ /backup/bronze/
docker exec lakehouse_minio mc mirror myminio/silver/ /backup/silver/
docker exec lakehouse_minio mc mirror myminio/gold/ /backup/gold/

# Restore
docker exec lakehouse_minio mc mirror /backup/bronze/ myminio/bronze/
```

#### 3. Hive Metastore

```bash
# Backup metastore database
docker exec lakehouse_postgres pg_dump -U lakehouse_user metastore_db > metastore_backup_$(date +%Y%m%d).sql
```

#### 4. MLflow

```bash
# Backup MLflow database
docker exec lakehouse_postgres pg_dump -U lakehouse_user mlflow_db > mlflow_backup_$(date +%Y%m%d).sql

# Backup artifacts
docker exec lakehouse_minio mc mirror myminio/gold/mlflow/ /backup/mlflow/
```

### Disaster Recovery

**Full System Restore**:
1. Install Docker & Docker Compose
2. Clone repository
3. Restore PostgreSQL databases
4. Restore MinIO buckets
5. Start services: `docker-compose up -d`
6. Verify health checks

---

## Contributing

Contributions are welcome! Please follow these guidelines:

### Code Style

- **Python**: Follow PEP 8, use type hints
- **SQL**: Uppercase keywords, lowercase table/column names
- **Comments**: Vietnamese OK cho business logic

### Pull Request Process

1. Fork repository
2. Create feature branch: `git checkout -b feature/my-feature`
3. Commit changes: `git commit -m "Add: my feature"`
4. Push to branch: `git push origin feature/my-feature`
5. Submit Pull Request với description đầy đủ

### Testing

- Test Spark jobs locally trước khi commit
- Verify DAG syntax: `python dags/<dag_file>.py`
- Check code với linter: `flake8 spark/jobs/`

---

## Roadmap

### Phase 1: Foundation (✅ Completed)
- ✅ Setup infrastructure với Docker Compose
- ✅ Implement Bronze → Silver → Gold pipeline
- ✅ Integrate Hive Metastore + Iceberg
- ✅ Setup Airflow orchestration

### Phase 2: ML & Analytics (✅ Completed)
- ✅ NLP processing cho comments (Underthesea)
- ✅ XGBoost time-series forecasting
- ✅ MLflow integration
- ✅ Gradio web app

### Phase 3: Enhancements (🚧 In Progress)
- 🚧 Real-time streaming với Kafka
- 🚧 Advanced NLP: Topic modeling, entity extraction
- 🚧 Dashboard với Superset/Metabase
- 🚧 API endpoints cho external applications

### Phase 4: Production (📅 Planned)
- 📅 Kubernetes deployment
- 📅 CI/CD pipeline (GitHub Actions)
- 📅 Data quality checks (Great Expectations)
- 📅 Performance monitoring (Prometheus + Grafana)
- 📅 Auto-scaling cho Spark executors

---

## License

This project is licensed under the MIT License - see LICENSE file for details.

---

## Acknowledgments

**Technologies**:
- [Apache Spark](https://spark.apache.org/) - Distributed processing
- [Apache Airflow](https://airflow.apache.org/) - Workflow orchestration
- [Apache Iceberg](https://iceberg.apache.org/) - Table format
- [Dremio](https://www.dremio.com/) - Query engine
- [MinIO](https://min.io/) - S3-compatible storage
- [MLflow](https://mlflow.org/) - ML lifecycle management
- [Gradio](https://gradio.app/) - ML web interfaces
- [Underthesea](https://github.com/undertheseanlp/underthesea) - Vietnamese NLP

**Data Sources**:
- Booking.com (public datasets)
- TikTok (research purposes)

---

## Contact & Support

**Project Repository**: <repository-url>  
**Issues**: <repository-url>/issues  
**Documentation**: See `/docs` folder (if available)

---

<div align="center">

**Built with ❤️ for Vietnamese Tourism Analytics**

⭐ Star this repo if you find it useful!

</div>
