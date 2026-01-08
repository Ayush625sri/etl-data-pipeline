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

def get_last_sync_time(entity_type):
    try:
        with open(f'/opt/airflow/data/.last_sync_{entity_type}', 'r') as f:
            return f.read().strip()
    except:
        return '1970-01-01 00:00:00'

def set_last_sync_time(entity_type):
    with open(f'/opt/airflow/data/.last_sync_{entity_type}', 'w') as f:
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

def load_addresses(**kwargs):
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT address_id, customer_id, address_type, city, state, country, is_default
        FROM shipping_addresses
    """)
    addresses = cursor.fetchall()
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        for addr in addresses:
            session.run("""
                MERGE (a:Address {id: $address_id})
                SET a.type = $address_type,
                    a.city = $city,
                    a.state = $state,
                    a.country = $country,
                    a.is_default = $is_default
            """, address_id=addr['address_id'],
                 address_type=addr['address_type'],
                 city=addr['city'],
                 state=addr['state'],
                 country=addr['country'],
                 is_default=addr['is_default'])
            
            session.run("""
                MATCH (c:Customer {id: $customer_id})
                MATCH (a:Address {id: $address_id})
                MERGE (c)-[:SHIPS_TO]->(a)
            """, customer_id=addr['customer_id'], address_id=addr['address_id'])
    
    cursor.close()
    mysql_conn.close()
    neo4j_driver.close()
    print(f"✓ Loaded {len(addresses)} addresses")

def load_orders(**kwargs):
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    cursor = mysql_conn.cursor(dictionary=True)
    last_sync = get_last_sync_time('orders')
    
    cursor.execute(f"""
        SELECT order_id, customer_id, address_id, order_date, status, 
               payment_status, total_amount, discount_amount, tax_amount
        FROM orders 
        WHERE order_date > '{last_sync}'
    """)
    orders = cursor.fetchall()
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        for order in orders:
            session.run("""
                MERGE (o:Order {id: $order_id})
                SET o.order_date = datetime($order_date),
                    o.status = $status,
                    o.payment_status = $payment_status,
                    o.total_amount = $total_amount,
                    o.discount_amount = $discount_amount,
                    o.tax_amount = $tax_amount
            """, order_id=order['order_id'],
                 order_date=order['order_date'].isoformat(),
                 status=order['status'],
                 payment_status=order['payment_status'],
                 total_amount=float(order['total_amount']),
                 discount_amount=float(order['discount_amount']),
                 tax_amount=float(order['tax_amount']))
            
            session.run("""
                MATCH (c:Customer {id: $customer_id})
                MATCH (o:Order {id: $order_id})
                MERGE (c)-[:PLACED {timestamp: datetime($order_date)}]->(o)
            """, customer_id=order['customer_id'], 
                 order_id=order['order_id'],
                 order_date=order['order_date'].isoformat())
            
            session.run("""
                MATCH (o:Order {id: $order_id})
                MATCH (a:Address {id: $address_id})
                MERGE (o)-[:SHIPPED_TO]->(a)
            """, order_id=order['order_id'], address_id=order['address_id'])
    
    set_last_sync_time('orders')
    cursor.close()
    mysql_conn.close()
    neo4j_driver.close()
    print(f"✓ Loaded {len(orders)} orders")

def load_order_items(**kwargs):
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT oi.order_id, oi.product_id, oi.quantity, oi.unit_price, oi.discount
        FROM order_items oi
    """)
    items = cursor.fetchall()
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        for item in items:
            session.run("""
                MATCH (o:Order {id: $order_id})
                MATCH (p:Product {id: $product_id})
                MERGE (o)-[r:CONTAINS]->(p)
                SET r.quantity = $quantity,
                    r.unit_price = $unit_price,
                    r.discount = $discount
            """, order_id=item['order_id'],
                 product_id=item['product_id'],
                 quantity=item['quantity'],
                 unit_price=float(item['unit_price']),
                 discount=float(item['discount']))
    
    cursor.close()
    mysql_conn.close()
    neo4j_driver.close()
    print(f"✓ Loaded {len(items)} order items")

with DAG(
    'transactions_pipeline',
    default_args=default_args,
    description='Load transactions to Neo4j',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    # wait_core = ExternalTaskSensor(
    #     task_id='wait_for_core_entities',
    #     external_dag_id='core_entities_pipeline',
    #     external_task_id='load_products',
    #     timeout=600
    # )
    
    addresses = PythonOperator(task_id='load_addresses', python_callable=load_addresses)
    orders = PythonOperator(task_id='load_orders', python_callable=load_orders)
    items = PythonOperator(task_id='load_order_items', python_callable=load_order_items)
    
    addresses >> orders >> items