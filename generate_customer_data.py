import pandas as pd
import random

def generate_customer_data(n=3000):
    first_names = ['John', 'Emma', 'Michael', 'Sophia', 'William', 'Olivia', 'James', 'Ava', 'Robert', 'Isabella']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
    segments = ['Premium', 'Standard', 'Basic']
    countries = ['USA', 'Canada', 'UK', 'Germany', 'France', 'Australia', 'India', 'Japan']
    
    data = []
    for i in range(n):
        data.append({
            'customer_id': f'CUST{1000+i}',
            'first_name': random.choice(first_names),
            'last_name': random.choice(last_names),
            'email': f'customer{1000+i}@email.com',
            'age': random.randint(18, 75),
            'country': random.choice(countries),
            'segment': random.choices(segments, weights=[20, 50, 30])[0],
            'loyalty_score': random.randint(0, 100),
            'total_purchases': random.randint(0, 50),
            'account_created': f'202{random.randint(0,4)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}',
            'is_active': random.choice([True, False])
        })
    
    df = pd.DataFrame(data)
    df.to_csv('data/raw_customers.csv', index=False)
    print(f"✓ Generated {n} customer records")

if __name__ == "__main__":
    generate_customer_data()