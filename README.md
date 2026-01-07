# Airflow ETL Pipeline Demo

## Overview
ETL pipeline using Apache Airflow to process sales data.

**Pipeline Flow:** Extract CSV → Transform data → Load summary

## Prerequisites
- Docker Desktop
- Python 3.8+

## Setup

1. **Clone/Download project**

2. **Start Airflow:**
```bash
docker-compose up -d
```

3. **Generate dummy data:**
```bash
python -m venv venv
venv\Scripts\activate  # Windows
pip install -r requirements.txt
python generate_dummy_data.py
```

4. **Access Airflow UI:**
- URL: http://localhost:8080
- Username: `airflow`
- Password: `airflow`

5. **Run pipeline:**
- Enable the `sales_etl_pipeline` DAG
- Click "Trigger DAG"

## Project Structure
```
├── dags/                   # Airflow DAG files
├── data/                   # Input/output data
├── docker-compose.yaml     # Airflow setup
└── generate_dummy_data.py  # Data generator
```

## Pipeline Steps
1. **Extract:** Read raw sales CSV
2. **Transform:** Calculate totals, remove duplicates
3. **Load:** Generate regional summary

## Stopping Airflow
```bash
docker-compose down
```