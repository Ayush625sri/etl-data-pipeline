from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_customers(**kwargs):
    df = pd.read_csv('/opt/airflow/data/raw_customers.csv')
    df = df[df['is_active'] == True]
    df.to_csv('/opt/airflow/data/extracted_customers.csv', index=False)
    print(f"✓ Extracted {len(df)} active customers")

def segment_customers(**kwargs):
    df = pd.read_csv('/opt/airflow/data/extracted_customers.csv')
    
    # Segmentation logic
    df['value_segment'] = pd.cut(df['loyalty_score'], bins=[0, 30, 70, 100], labels=['Low', 'Medium', 'High'])
    
    df['age_group'] = pd.cut(df['age'], bins=[0, 25, 45, 65, 100], labels=['Young', 'Adult', 'Middle', 'Senior'])
    
    df.to_csv('/opt/airflow/data/segmented_customers.csv', index=False)
    print(f"✓ Segmented {len(df)} customers")

def load_segments(**kwargs):
    df = pd.read_csv('/opt/airflow/data/segmented_customers.csv')
    
    segment_summary = df.groupby(['segment', 'value_segment']).agg({
        'customer_id': 'count',
        'loyalty_score': 'mean',
        'total_purchases': 'sum'
    }).reset_index()
    
    segment_summary.to_csv('/opt/airflow/data/customer_segments.csv', index=False)
    print(f"✓ Loaded {len(segment_summary)} segment groups")

with DAG(
    'customer_segmentation',
    default_args=default_args,
    description='Segment customers by behavior',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    extract = PythonOperator(task_id='extract_customers', python_callable=extract_customers)
    segment = PythonOperator(task_id='segment_customers', python_callable=segment_customers)
    load = PythonOperator(task_id='load_segments', python_callable=load_segments)
    
    extract >> segment >> load