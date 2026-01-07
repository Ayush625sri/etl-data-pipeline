import pandas as pd
import random

def generate_product_data(n=2000):
    categories = ['Electronics', 'Accessories', 'Computing', 'Audio', 'Gaming']
    suppliers = ['TechCorp', 'GlobalSupply', 'MegaDistributor', 'DirectSource', 'PrimeVendor']
    brands = ['BrandA', 'BrandB', 'BrandC', 'BrandD', 'BrandE']
    
    data = []
    for i in range(n):
        cost = round(random.uniform(20, 1500), 2)
        price = round(cost * random.uniform(1.3, 2.5), 2)
        
        data.append({
            'product_id': f'PROD{5000+i}',
            'product_name': f'Product_{i}',
            'category': random.choice(categories),
            'brand': random.choice(brands),
            'supplier': random.choice(suppliers),
            'cost_price': cost,
            'selling_price': price,
            'stock_quantity': random.randint(0, 500),
            'reorder_level': random.randint(10, 50),
            'weight_kg': round(random.uniform(0.1, 5.0), 2),
            'is_discontinued': random.choices([True, False], weights=[10, 90])[0],
            'warehouse_location': f'WH{random.randint(1,5)}-A{random.randint(1,20)}'
        })
    
    df = pd.DataFrame(data)
    df.to_csv('data/raw_products.csv', index=False)
    print(f"✓ Generated {n} product records")

if __name__ == "__main__":
    generate_product_data()