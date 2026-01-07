from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_data(**kwargs):
    """Extract: Read raw sales data from CSV"""
    df = pd.read_csv('/opt/airflow/data/raw_sales.csv')
    df.to_csv('/opt/airflow/data/extracted_sales.csv', index=False)
    print(f"✓ Extracted {len(df)} records")
    return len(df)

def transform_data(**kwargs):
    """Transform: Clean data and calculate total amounts"""
    df = pd.read_csv('/opt/airflow/data/extracted_sales.csv')
    
    # Calculate total amount per order
    df['total_amount'] = df['quantity'] * df['price']
    
    # Remove duplicates
    df = df.drop_duplicates(subset=['order_id'])
    
    # Convert date to datetime
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    df.to_csv('/opt/airflow/data/transformed_sales.csv', index=False)
    print(f"✓ Transformed {len(df)} records")
    return len(df)

def load_data(**kwargs):
    """Load: Generate regional sales summary"""
    df = pd.read_csv('/opt/airflow/data/transformed_sales.csv')
    
    # Aggregate by region
    summary = df.groupby('region').agg({
        'total_amount': 'sum',
        'quantity': 'sum',
        'order_id': 'count'
    }).reset_index()
    
    summary.columns = ['region', 'total_revenue', 'total_quantity', 'order_count']
    summary['avg_order_value'] = summary['total_revenue'] / summary['order_count']
    
    summary.to_csv('/opt/airflow/data/sales_summary.csv', index=False)
    print(f"✓ Loaded summary for {len(summary)} regions")
    print(summary)
    return summary.to_dict()

# Define DAG
dag = DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for sales data analysis',
    schedule_interval='@daily',  # Runs daily
    catchup=False,
)

# Define tasks
extract_task = PythonOperator(
    task_id='extract_sales_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_sales_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_sales_summary',
    python_callable=load_data,
    dag=dag,
)

# Set task dependencies: Extract → Transform → Load
extract_task >> transform_task >> load_task