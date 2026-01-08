from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_sales(**kwargs):
    df = pd.read_csv('/opt/airflow/data/raw_sales.csv')
    if df.empty:
        raise ValueError("Empty sales data")
    df.to_csv('/opt/airflow/data/extracted_sales.csv', index=False)
    print(f"✓ Extracted {len(df)} sales records")
    kwargs['ti'].xcom_push(key='sales_count', value=len(df))

def transform_sales(**kwargs):
    df = pd.read_csv('/opt/airflow/data/extracted_sales.csv')
    
    # Data quality checks
    if df['price'].isnull().sum() > 0:
        raise ValueError("Null prices detected")
    
    df = df[df['status'] == 'Completed']
    df['total_amount'] = df['quantity'] * df['price']
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    df.to_csv('/opt/airflow/data/transformed_sales.csv', index=False)
    print(f"✓ Transformed {len(df)} sales records")

def load_sales_summary(**kwargs):
    df = pd.read_csv('/opt/airflow/data/transformed_sales.csv')
    
    summary = df.groupby('region').agg({
        'total_amount': 'sum',
        'quantity': 'sum',
        'order_id': 'count'
    }).reset_index()
    
    summary.columns = ['region', 'total_revenue', 'total_quantity', 'order_count']
    summary.to_csv('/opt/airflow/data/sales_summary.csv', index=False)
    print(f"✓ Loaded summary: {len(summary)} regions")

with DAG(
    'sales_processing',
    default_args=default_args,
    description='Process sales data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    extract = PythonOperator(task_id='extract_sales', python_callable=extract_sales)
    transform = PythonOperator(task_id='transform_sales', python_callable=transform_sales)
    load = PythonOperator(task_id='load_sales_summary', python_callable=load_sales_summary)
    
    extract >> transform >> load