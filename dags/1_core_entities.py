from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import mysql.connector
from neo4j import GraphDatabase
import sys
import os

sys.path.insert(0, '/opt/airflow')
from config.settings import MYSQL_CONFIG, NEO4J_CONFIG

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def get_last_sync_time(entity_type):
    """Get last sync timestamp from file"""
    try:
        with open(f'/opt/airflow/data/.last_sync_{entity_type}', 'r') as f:
            return f.read().strip()
    except:
        return '1970-01-01 00:00:00'

def set_last_sync_time(entity_type):
    """Save current sync timestamp"""
    with open(f'/opt/airflow/data/.last_sync_{entity_type}', 'w') as f:
        f.write(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

def load_customers(**kwargs):
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    cursor = mysql_conn.cursor(dictionary=True)
    last_sync = get_last_sync_time('customers')
    
    cursor.execute(f"""
        SELECT customer_id, email, first_name, last_name, phone, age, gender, 
               segment, loyalty_points, account_status, registration_date
        FROM customers 
        WHERE created_at > '{last_sync}'
    """)
    
    customers = cursor.fetchall()
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        for customer in customers:
            session.run("""
                MERGE (c:Customer {id: $customer_id})
                SET c.email = $email,
                    c.first_name = $first_name,
                    c.last_name = $last_name,
                    c.phone = $phone,
                    c.age = $age,
                    c.gender = $gender,
                    c.segment = $segment,
                    c.loyalty_points = $loyalty_points,
                    c.account_status = $account_status,
                    c.registration_date = date($registration_date),
                    c.updated_at = datetime()
            """, customer_id=customer['customer_id'],
                 email=customer['email'],
                 first_name=customer['first_name'],
                 last_name=customer['last_name'],
                 phone=customer['phone'],
                 age=customer['age'],
                 gender=customer['gender'],
                 segment=customer['segment'],
                 loyalty_points=customer['loyalty_points'],
                 account_status=customer['account_status'],
                 registration_date=str(customer['registration_date']))
    
    set_last_sync_time('customers')
    cursor.close()
    mysql_conn.close()
    neo4j_driver.close()
    print(f"✓ Loaded {len(customers)} customers to Neo4j")

def load_categories(**kwargs):
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute("SELECT category_id, name, description, parent_category_id FROM categories")
    categories = cursor.fetchall()
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        for cat in categories:
            session.run("""
                MERGE (c:Category {id: $category_id})
                SET c.name = $name,
                    c.description = $description
            """, category_id=cat['category_id'], name=cat['name'], 
                description=cat['description'])
            
            if cat['parent_category_id']:
                session.run("""
                    MATCH (child:Category {id: $child_id})
                    MATCH (parent:Category {id: $parent_id})
                    MERGE (child)-[:SUBCATEGORY_OF]->(parent)
                """, child_id=cat['category_id'], parent_id=cat['parent_category_id'])
    
    cursor.close()
    mysql_conn.close()
    neo4j_driver.close()
    print(f"✓ Loaded {len(categories)} categories")

def load_suppliers(**kwargs):
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    cursor = mysql_conn.cursor(dictionary=True)
    cursor.execute("SELECT supplier_id, name, country, contact_email, rating FROM suppliers")
    suppliers = cursor.fetchall()
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        for sup in suppliers:
            session.run("""
                MERGE (s:Supplier {id: $supplier_id})
                SET s.name = $name,
                    s.country = $country,
                    s.contact_email = $contact_email,
                    s.rating = $rating
            """, supplier_id=sup['supplier_id'],
                name=sup['name'],
                country=sup['country'],
                contact_email=sup['contact_email'],
                rating=float(sup['rating']))
    
    cursor.close()
    mysql_conn.close()
    neo4j_driver.close()
    print(f"✓ Loaded {len(suppliers)} suppliers")

def load_products(**kwargs):
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    neo4j_driver = GraphDatabase.driver(NEO4J_CONFIG['uri'], 
                                        auth=(NEO4J_CONFIG['user'], NEO4J_CONFIG['password']))
    
    cursor = mysql_conn.cursor(dictionary=True)
    last_sync = get_last_sync_time('products')
    
    cursor.execute(f"""
        SELECT product_id, name, sku, brand, category_id, supplier_id, 
               price, cost, weight_kg, rating_avg, total_reviews
        FROM products 
        WHERE updated_at > '{last_sync}'
    """)
    
    products = cursor.fetchall()
    
    with neo4j_driver.session(database=NEO4J_CONFIG['database']) as session:
        for prod in products:
            session.run("""
                MERGE (p:Product {id: $product_id})
                SET p.name = $name,
                    p.sku = $sku,
                    p.brand = $brand,
                    p.price = $price,
                    p.cost = $cost,
                    p.weight_kg = $weight_kg,
                    p.rating_avg = $rating_avg,
                    p.total_reviews = $total_reviews,
                    p.updated_at = datetime()
            """, product_id=prod['product_id'],
                 name=prod['name'],
                 sku=prod['sku'],
                 brand=prod['brand'],
                 price=float(prod['price']),
                 cost=float(prod['cost']),
                 weight_kg=float(prod['weight_kg']),
                 rating_avg=float(prod['rating_avg']),
                 total_reviews=prod['total_reviews'])
            
            session.run("""
                MATCH (p:Product {id: $product_id})
                MATCH (c:Category {id: $category_id})
                MERGE (p)-[:BELONGS_TO]->(c)
            """, product_id=prod['product_id'], category_id=prod['category_id'])
            
            session.run("""
                MATCH (p:Product {id: $product_id})
                MATCH (s:Supplier {id: $supplier_id})
                MERGE (p)-[:SUPPLIED_BY]->(s)
            """, product_id=prod['product_id'], supplier_id=prod['supplier_id'])
    
    set_last_sync_time('products')
    cursor.close()
    mysql_conn.close()
    neo4j_driver.close()
    print(f"✓ Loaded {len(products)} products")

with DAG(
    'core_entities_pipeline',
    default_args=default_args,
    description='Load core entities to Neo4j',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    customers = PythonOperator(task_id='load_customers', python_callable=load_customers)
    categories = PythonOperator(task_id='load_categories', python_callable=load_categories)
    suppliers = PythonOperator(task_id='load_suppliers', python_callable=load_suppliers)
    products = PythonOperator(task_id='load_products', python_callable=load_products)
    
    [customers, categories, suppliers] >> products