from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import mysql.connector
from neo4j import GraphDatabase
import sys

sys.path.insert(0, '/opt/airflow')
from config.settings import MYSQL_CONFIG, NEO4J_CONFIG

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def load_reviews(**kwargs):
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT review_id, customer_id, product_id, rating, title, 
               verified_purchase, helpful_count, review_date
        FROM reviews
    """)
    reviews = cursor.fetchall()
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        for review in reviews:
            session.run("""
                MATCH (c:Customer {id: $customer_id})
                MATCH (p:Product {id: $product_id})
                MERGE (c)-[r:REVIEWED {id: $review_id}]->(p)
                SET r.rating = $rating,
                    r.title = $title,
                    r.verified_purchase = $verified_purchase,
                    r.helpful_count = $helpful_count,
                    r.review_date = datetime($review_date)
            """, review_id=review['review_id'],
                 customer_id=review['customer_id'],
                 product_id=review['product_id'],
                 rating=review['rating'],
                 title=review['title'],
                 verified_purchase=review['verified_purchase'],
                 helpful_count=review['helpful_count'],
                 review_date=review['review_date'].isoformat())
    
    cursor.close()
    mysql_conn.close()
    neo4j_driver.close()
    print(f"✓ Loaded {len(reviews)} reviews")

def load_payments(**kwargs):
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT transaction_id, order_id, payment_method, amount, status
        FROM payment_transactions
    """)
    payments = cursor.fetchall()
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        for payment in payments:
            session.run("""
                MERGE (pm:PaymentMethod {name: $payment_method})
            """, payment_method=payment['payment_method'])
            
            session.run("""
                MATCH (o:Order {id: $order_id})
                MATCH (pm:PaymentMethod {name: $payment_method})
                MERGE (o)-[r:PAID_VIA]->(pm)
                SET r.amount = $amount,
                    r.status = $status,
                    r.transaction_id = $transaction_id
            """, order_id=payment['order_id'],
                 payment_method=payment['payment_method'],
                 amount=float(payment['amount']),
                 status=payment['status'],
                 transaction_id=payment['transaction_id'])
    
    cursor.close()
    mysql_conn.close()
    neo4j_driver.close()
    print(f"✓ Loaded {len(payments)} payments")

def load_inventory(**kwargs):
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT inventory_id, product_id, warehouse_location, stock_quantity, reorder_level
        FROM inventory
    """)
    inventory = cursor.fetchall()
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        for inv in inventory:
            session.run("""
                MERGE (w:Warehouse {location: $warehouse_location})
            """, warehouse_location=inv['warehouse_location'])
            
            session.run("""
                MATCH (p:Product {id: $product_id})
                MATCH (w:Warehouse {location: $warehouse_location})
                MERGE (p)-[r:STORED_AT]->(w)
                SET r.stock_quantity = $stock_quantity,
                    r.reorder_level = $reorder_level
            """, product_id=inv['product_id'],
                 warehouse_location=inv['warehouse_location'],
                 stock_quantity=inv['stock_quantity'],
                 reorder_level=inv['reorder_level'])
    
    cursor.close()
    mysql_conn.close()
    neo4j_driver.close()
    print(f"✓ Loaded {len(inventory)} inventory records")

with DAG(
    'enrichment_pipeline',
    default_args=default_args,
    description='Load enrichment data to Neo4j',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    # wait_transactions = ExternalTaskSensor(
    #     task_id='wait_for_transactions',
    #     external_dag_id='transactions_pipeline',
    #     external_task_id='load_order_items',
    #     timeout=600
    # )
    
    reviews = PythonOperator(task_id='load_reviews', python_callable=load_reviews)
    payments = PythonOperator(task_id='load_payments', python_callable=load_payments)
    inventory = PythonOperator(task_id='load_inventory', python_callable=load_inventory)
    
    [reviews, payments, inventory]