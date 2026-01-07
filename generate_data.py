import pandas as pd
import random
from datetime import datetime, timedelta

def generate_sales_data(num_records=100):
    data = []
    products = ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Monitor']
    regions = ['North', 'South', 'East', 'West']
    
    start_date = datetime.now() - timedelta(days=30)
    
    for i in range(num_records):
        data.append({
            'order_id': f'ORD{1000+i}',
            'product': random.choice(products),
            'quantity': random.randint(1, 10),
            'price': round(random.uniform(50, 2000), 2),
            'region': random.choice(regions),
            'order_date': (start_date + timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d')
        })
    
    df = pd.DataFrame(data)
    df.to_csv('data/raw_sales.csv', index=False)
    print(f"Generated {num_records} records")

if __name__ == "__main__":
    generate_sales_data()