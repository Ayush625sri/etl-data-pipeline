# ETL Data Pipeline: MySQL to Neo4j

<!-- [![GitHub](https://img.shields.io/badge/GitHub-etl--data--pipeline-blue)](https://github.com/Ayush625sri/etl-data-pipeline) -->

Apache Airflow-orchestrated ETL pipeline transforming e-commerce data from MySQL relational database into Neo4j knowledge graph for advanced analytics.

## 🎯 Project Overview

**Objective:** Demonstrate production-grade ETL pipeline converting structured data into graph relationships for complex query patterns and business intelligence.

**Key Features:**
- 4 automated data pipelines
- 10 MySQL tables → 8 Neo4j node types
- 12 relationship types with derived analytics
- Incremental data synchronization
- Error handling and retry logic

## 📊 Data Flow
```
MySQL (10 tables, 10K+ records)
    ↓
Apache Airflow (4 DAG pipelines)
    ↓
Neo4j Graph (8 nodes, 12 relationships)
```

## 🏗️ Architecture

**Pipeline 1:** Core entities (customers, products, categories, suppliers)  
**Pipeline 2:** Transactions (orders, order items, addresses)  
**Pipeline 3:** Enrichment (reviews, payments, inventory)  
**Pipeline 4:** Analytics (co-purchases, customer similarity, metrics)

## 🚀 Quick Start

### Prerequisites
- Docker Desktop
- MySQL 8.0
- Neo4j Desktop
- Python 3.8+

### Setup

1. Clone repository:
```bash
git clone https://github.com/Ayush625sri/etl-data-pipeline
cd etl-data-pipeline
```

2. Configure connections in `config/settings.py`

3. Generate MySQL data:
```bash
python -m setup.schema
python -m setup.generate_data
```

4. Start Airflow:
```bash
docker-compose up -d
```

5. Access Airflow UI: http://localhost:8080 (airflow/airflow)

6. Trigger pipelines in order: core_entities → transactions → enrichment → analytics

## 📁 Project Structure
```
etl-data-pipeline/
├── dags/
│   ├── 1_core_entities.py
│   ├── 2_transactions.py
│   ├── 3_enrichment.py
│   └── 4_analytics.py
├── setup/
│   ├── schema.py
│   └── generate_data.py
├── config/
│   └── settings.py
├── docker-compose.yaml
├── requirements.txt
├── README.md
└── DOCUMENTATION.md
```

## 📈 Results

**Nodes Created:** 6,035  
**Relationships Created:** 16,718  
**Data Sources:** 10 MySQL tables  
**Pipeline Execution:** Daily (1-3) + Weekly (4)

## 🔍 Sample Queries

**Customer purchase journey:**
```cypher
MATCH path = (c:Customer)-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
WHERE c.id = 1
RETURN path
```

**Product recommendations:**
```cypher
MATCH (p1:Product)-[:BOUGHT_TOGETHER]->(p2:Product)
RETURN p1.name, p2.name
ORDER BY p2.name
```

## 📚 Documentation

Complete documentation available in [DOCUMENTATION.md](DOCUMENTATION.md)

## 🛠️ Tech Stack

- **Orchestration:** Apache Airflow 2.8.1
- **Source DB:** MySQL 8.0
- **Target DB:** Neo4j
- **Language:** Python 3.8
- **Deployment:** Docker Compose
