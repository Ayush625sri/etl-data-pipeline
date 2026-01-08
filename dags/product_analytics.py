from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_products(**kwargs):
    df = pd.read_csv('/opt/airflow/data/raw_products.csv')
    df = df[df['is_discontinued'] == False]
    df.to_csv('/opt/airflow/data/extracted_products.csv', index=False)
    print(f"✓ Extracted {len(df)} active products")

def analyze_inventory(**kwargs):
    df = pd.read_csv('/opt/airflow/data/extracted_products.csv')
    
    # Inventory analysis
    df['needs_reorder'] = df['stock_quantity'] < df['reorder_level']
    df['profit_margin'] = ((df['selling_price'] - df['cost_price']) / df['cost_price'] * 100).round(2)
    df['stock_value'] = df['stock_quantity'] * df['cost_price']
    
    df.to_csv('/opt/airflow/data/analyzed_products.csv', index=False)
    print(f"✓ Analyzed {len(df)} products")

def load_inventory_report(**kwargs):
    df = pd.read_csv('/opt/airflow/data/analyzed_products.csv')
    
    report = df.groupby('category').agg({
        'product_id': 'count',
        'stock_quantity': 'sum',
        'stock_value': 'sum',
        'profit_margin': 'mean'
    }).reset_index()
    
    report.columns = ['category', 'product_count', 'total_stock', 'total_value', 'avg_margin']
    report.to_csv('/opt/airflow/data/inventory_report.csv', index=False)
    print(f"✓ Generated inventory report")

with DAG(
    'product_analytics',
    default_args=default_args,
    description='Analyze product inventory',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    extract = PythonOperator(task_id='extract_products', python_callable=extract_products)
    analyze = PythonOperator(task_id='analyze_inventory', python_callable=analyze_inventory)
    load = PythonOperator(task_id='load_inventory_report', python_callable=load_inventory_report)
    
    extract >> analyze >> load