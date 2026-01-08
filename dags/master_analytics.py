from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def wait_for_pipelines(**kwargs):
    print("✓ All upstream pipelines completed")

def combine_all_data(**kwargs):
    sales = pd.read_csv('/opt/airflow/data/transformed_sales.csv')
    customers = pd.read_csv('/opt/airflow/data/segmented_customers.csv')
    products = pd.read_csv('/opt/airflow/data/analyzed_products.csv')
    
    # Merge datasets
    combined = sales.merge(customers[['customer_id', 'segment', 'value_segment']], 
                          on='customer_id', how='left')
    
    combined.to_csv('/opt/airflow/data/combined_data.csv', index=False)
    print(f"✓ Combined {len(combined)} records")

def generate_insights(**kwargs):
    df = pd.read_csv('/opt/airflow/data/combined_data.csv')
    
    # Business insights
    insights = df.groupby(['region', 'segment']).agg({
        'total_amount': ['sum', 'mean'],
        'quantity': 'sum',
        'order_id': 'count'
    }).reset_index()
    
    insights.columns = ['region', 'segment', 'total_revenue', 'avg_order_value', 'total_quantity', 'order_count']
    insights.to_csv('/opt/airflow/data/business_insights.csv', index=False)
    print(f"✓ Generated {len(insights)} insight rows")

def load_final_report(**kwargs):
    insights = pd.read_csv('/opt/airflow/data/business_insights.csv')
    sales_summary = pd.read_csv('/opt/airflow/data/sales_summary.csv')
    inventory = pd.read_csv('/opt/airflow/data/inventory_report.csv')
    
    # Create executive summary
    report = {
        'total_revenue': insights['total_revenue'].sum(),
        'total_orders': insights['order_count'].sum(),
        'avg_order_value': insights['avg_order_value'].mean(),
        'total_inventory_value': inventory['total_value'].sum()
    }
    
    pd.DataFrame([report]).to_csv('/opt/airflow/data/executive_report.csv', index=False)
    print("✓ Final report generated")

with DAG(
    'master_analytics',
    default_args=default_args,
    description='Combined analytics from all pipelines',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    wait_sales = ExternalTaskSensor(
        task_id='wait_for_sales',
        external_dag_id='sales_processing',
        external_task_id='load_sales_summary',
        timeout=600
    )
    
    wait_customers = ExternalTaskSensor(
        task_id='wait_for_customers',
        external_dag_id='customer_segmentation',
        external_task_id='load_segments',
        timeout=600
    )
    
    wait_products = ExternalTaskSensor(
        task_id='wait_for_products',
        external_dag_id='product_analytics',
        external_task_id='load_inventory_report',
        timeout=600
    )
    
    combine = PythonOperator(task_id='combine_all_data', python_callable=combine_all_data)
    insights = PythonOperator(task_id='generate_insights', python_callable=generate_insights)
    final = PythonOperator(task_id='load_final_report', python_callable=load_final_report)
    
    [wait_sales, wait_customers, wait_products] >> combine >> insights >> final