from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from neo4j import GraphDatabase
import sys

sys.path.insert(0, '/opt/airflow')
from config.settings import NEO4J_CONFIG

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def calculate_co_purchases(**kwargs):
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        session.run("""
            MATCH (o:Order)-[:CONTAINS]->(p1:Product)
            MATCH (o)-[:CONTAINS]->(p2:Product)
            WHERE p1.id < p2.id
            WITH p1, p2, COUNT(o) as purchase_count
            WHERE purchase_count >= 2
            MERGE (p1)-[r:BOUGHT_TOGETHER]->(p2)
            SET r.count = purchase_count
        """)
    
    neo4j_driver.close()
    print("✓ Calculated co-purchase relationships")

def calculate_customer_similarity(**kwargs):
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        session.run("""
            MATCH (c1:Customer)-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)
            MATCH (c2:Customer)-[:PLACED]->(:Order)-[:CONTAINS]->(p)
            WHERE c1.id < c2.id AND c1.segment = c2.segment
            WITH c1, c2, COUNT(DISTINCT p) as common_products
            WHERE common_products >= 2
            MERGE (c1)-[r:SIMILAR_TO]->(c2)
            SET r.common_products = common_products
        """)
    
    neo4j_driver.close()
    print("✓ Calculated customer similarity")

def calculate_product_metrics(**kwargs):
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        session.run("""
            MATCH (p:Product)<-[c:CONTAINS]-(:Order)
            WITH p, SUM(c.quantity) as total_sold, 
                 SUM(c.quantity * c.unit_price) as total_revenue,
                 COUNT(*) as order_count
            SET p.total_sold = total_sold,
                p.total_revenue = total_revenue,
                p.order_count = order_count
        """)
    
    neo4j_driver.close()
    print("✓ Updated product metrics")

with DAG(
    'analytics_pipeline',
    default_args=default_args,
    description='Calculate derived relationships and analytics',
    schedule_interval='@weekly',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    # wait_enrichment = ExternalTaskSensor(
    #     task_id='wait_for_enrichment',
    #     external_dag_id='enrichment_pipeline',
    #     external_task_id='load_reviews',
    #     timeout=600,
    #     mode='reschedule'
    # )
    
    co_purchases = PythonOperator(task_id='calculate_co_purchases', 
                                  python_callable=calculate_co_purchases)
    similarity = PythonOperator(task_id='calculate_customer_similarity', 
                                python_callable=calculate_customer_similarity)
    metrics = PythonOperator(task_id='calculate_product_metrics', 
                            python_callable=calculate_product_metrics)
    
    [co_purchases, similarity, metrics]