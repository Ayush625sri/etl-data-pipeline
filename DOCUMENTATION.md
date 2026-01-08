# ETL Data Pipeline: MySQL to Neo4j

## Overview
ETL pipeline orchestrated by Apache Airflow that extracts e-commerce data from MySQL, transforms it, and loads into Neo4j knowledge graph for advanced analytics and relationship queries.

## Tech Stack
- **Source:** MySQL 8.0
- **Orchestration:** Apache Airflow 2.8.1 (Docker)
- **Target:** Neo4j Desktop
- **Language:** Python 3.8+
- **Libraries:** mysql-connector-python, neo4j, pandas, faker

## Architecture

### Data Flow
1. **MySQL Source:** 10 tables with e-commerce data (customers, products, orders, etc.)
2. **Airflow ETL:** 4 pipelines extract, transform, and load data
3. **Neo4j Target:** Graph database with 8 node types and 12 relationship types

### Pipeline Details

#### Pipeline 1: Core Entities (@daily)
**Purpose:** Load foundational entities

**Tables:** customers, products, categories, suppliers

**Transformations:**
- Convert MySQL Decimal to float
- Format dates to ISO string

**Neo4j Output:**
- Nodes: Customer, Product, Category, Supplier
- Relationships: BELONGS_TO, SUPPLIED_BY, SUBCATEGORY_OF

**Incremental Sync:** Tracks last sync time for customers and products

#### Pipeline 2: Transactions (@daily)
**Purpose:** Load transactional data

**Tables:** orders, order_items, shipping_addresses

**Transformations:**
- Convert datetime to ISO format
- Convert decimal amounts to float

**Neo4j Output:**
- Nodes: Order, Address
- Relationships: PLACED, CONTAINS, SHIPPED_TO, SHIPS_TO

**Incremental Sync:** Tracks last sync time for orders

#### Pipeline 3: Enrichment (@daily)
**Purpose:** Add supplementary data

**Tables:** reviews, payment_transactions, inventory

**Transformations:**
- Format review dates
- Convert payment amounts to float

**Neo4j Output:**
- Nodes: PaymentMethod, Warehouse
- Relationships: REVIEWED, PAID_VIA, STORED_AT

#### Pipeline 4: Analytics (@weekly)
**Purpose:** Calculate derived relationships

**Calculations:**
1. **Co-purchases:** Products bought together in 3+ orders
2. **Customer similarity:** Customers with 3+ common products in same segment
3. **Product metrics:** Total sold, revenue, order count

**Neo4j Output:**
- Relationships: BOUGHT_TOGETHER, SIMILAR_TO
- Properties: total_sold, total_revenue, order_count on Product nodes

### Data Model

**MySQL Tables (10):**
- customers (1000 records)
- products (1000 records)
- categories (10 records)
- suppliers (50 records)
- orders (2000 records)
- order_items (5934 records)
- shipping_addresses (1971 records)
- reviews (800 records)
- payment_transactions (2000 records)
- inventory (1000 records)

**Neo4j Graph:**
- 8 node types
- 12 relationship types
- Total nodes: ~6,035
- Total relationships: ~16,718

## Setup Instructions

### Prerequisites
- Docker Desktop
- MySQL 8.0 (localhost:3306)
- Neo4j Desktop (localhost:7687)
- Python 3.8+

### Installation

1. **Clone repository:**
```bash
git clone https://github.com/Ayush625sri/etl-data-pipeline
cd etl-data-pipeline
```

2. **Update configurations:**
Edit `config/settings.py`:
```python
MYSQL_CONFIG = {
    'host': 'host.docker.internal',
    'user': 'root',
    'password': 'your_password',
    'database': 'ecommerce'
}

NEO4J_CONFIG = {
    'uri': 'bolt://host.docker.internal:7687',
    'user': 'neo4j',
    'password': 'your_password',
    'database': 'ecommerce'
}
```

3. **Setup MySQL database:**
```bash
python -m setup.schema
python -m setup.generate_data
```

4. **Create Neo4j database:**
- Open Neo4j Desktop
- Start instance
- Create database named `ecommerce`

5. **Start Airflow:**
```bash
docker-compose up -d
```
Access UI: http://localhost:8080 (airflow/airflow)

### Environment Configuration

1. **Copy environment template:**
```bash
cp .env.example .env
```

2. **Update `.env` with your credentials:**
```
AIRFLOW_UID=50000
_PIP_ADDITIONAL_REQUIREMENTS=mysql-connector-python neo4j pandas

# MySQL Configuration
MYSQL_HOST=host.docker.internal
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password
MYSQL_PORT=3306
MYSQL_DATABASE=ecommerce

# Neo4j Configuration
NEO4J_URI=bolt://host.docker.internal:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_neo4j_password
NEO4J_DATABASE=ecommerce
```

### Running Pipelines

**Manual Execution:**
1. Toggle all 4 DAGs ON
2. Trigger `core_entities_pipeline`
3. After completion, trigger `transactions_pipeline`
4. After completion, trigger `enrichment_pipeline`
5. After completion, trigger `analytics_pipeline`

**Scheduled Execution:**
- Pipelines 1-3 run @daily at midnight
- Pipeline 4 runs @weekly on Sunday

### Verification

**Check Neo4j data:**
```cypher
// Count nodes
MATCH (n) RETURN DISTINCT labels(n), COUNT(n)

// Count relationships
MATCH ()-[r]->() RETURN TYPE(r), COUNT(r)

// View sample graph
MATCH (c:Customer)-[:PLACED]->(o:Order)-[:CONTAINS]->(p:Product)
RETURN c, o, p LIMIT 25
```

## Data Quality

### Validation Checks
- Null value detection in prices
- Duplicate prevention via MERGE
- Data type conversion (Decimal → float)
- DateTime format standardization

### Error Handling
- Retry logic: 2 retries, 5-minute delay
- Incremental sync tracking
- Failed task logging

## Analytics Use Cases

### Sample Queries

**1. Customer purchase journey:**
```cypher
MATCH path = (c:Customer)-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
WHERE c.id = 1
RETURN path
```

**2. Product recommendations:**
```cypher
MATCH (p1:Product {name: 'Laptop'})-[:BOUGHT_TOGETHER]->(p2:Product)
RETURN p2.name, p2.price
ORDER BY p2.price
```

**3. Revenue by category:**
```cypher
MATCH (p:Product)-[:BELONGS_TO]->(c:Category)
RETURN c.name, SUM(p.total_revenue) as category_revenue
ORDER BY category_revenue DESC
```

**4. Similar customers:**
```cypher
MATCH (c1:Customer {id: 1})-[r:SIMILAR_TO]->(c2:Customer)
RETURN c2.first_name, c2.last_name, r.common_products
ORDER BY r.common_products DESC
```

**5. Top selling products:**
```cypher
MATCH (p:Product)
WHERE p.total_sold IS NOT NULL
RETURN p.name, p.total_sold, p.total_revenue
ORDER BY p.total_sold DESC
LIMIT 10
```

## Troubleshooting

### Common Issues

**1. MySQL connection error:**
- Verify MySQL is running: `mysql -u root -p`
- Check `host.docker.internal` resolves from Docker
- Alternative: Use machine's IP address

**2. Neo4j connection error:**
- Verify Neo4j instance is started
- Check database `ecommerce` exists
- Verify credentials match config

**3. Empty pipeline results:**
- Delete sync files: `rm /opt/airflow/data/.last_sync_*`
- Re-trigger pipeline


### Optimization
- Batch inserts (processes all records per table)
- MERGE prevents duplicates
- Incremental sync reduces load
- Parallel task execution where possible

## Maintenance

### Regular Tasks
- Monitor Airflow logs: `docker-compose logs -f`
- Clear old sync files weekly
- Verify data consistency monthly
- Update schemas as needed

### Backup
```bash
# MySQL backup
mysqldump -u root -p ecommerce > backup.sql

# Neo4j backup (from Neo4j Browser)
:dumpdb ecommerce
```

## Future Enhancements
- Real-time streaming with Kafka
- Data quality dashboard
- ML-based product recommendations
- Customer churn prediction
- Automated data validation tests