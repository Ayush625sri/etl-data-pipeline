import pandas as pd
import random
from datetime import datetime, timedelta

def generate_sales_data(n=5000):
    products = ['Laptop', 'Phone', 'Tablet', 'Headphones', 'Monitor', 'Keyboard', 'Mouse', 'Webcam']
    regions = ['North', 'South', 'East', 'West', 'Central']
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash']
    statuses = ['Completed', 'Pending', 'Cancelled', 'Refunded']
    
    start_date = datetime.now() - timedelta(days=365)
    
    data = []
    for i in range(n):
        order_date = start_date + timedelta(days=random.randint(0, 365))
        data.append({
            'order_id': f'ORD{10000+i}',
            'customer_id': f'CUST{random.randint(1000, 3999)}',
            'product': random.choice(products),
            'quantity': random.randint(1, 10),
            'price': round(random.uniform(50, 2000), 2),
            'region': random.choice(regions),
            'payment_method': random.choice(payment_methods),
            'status': random.choices(statuses, weights=[70, 15, 10, 5])[0],
            'order_date': order_date.strftime('%Y-%m-%d'),
            'shipping_cost': round(random.uniform(5, 50), 2)
        })
    
    df = pd.DataFrame(data)
    df.to_csv('data/raw_sales.csv', index=False)
    print(f"✓ Generated {n} sales records")

if __name__ == "__main__":
    generate_sales_data()