import mysql.connector
from faker import Faker
import random
from datetime import datetime, timedelta
from config.settings import MYSQL_CONFIG

fake = Faker()

def generate_data():
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    
    # Categories
    categories_data = [
        ('Electronics', 'Electronic devices and gadgets', None),
        ('Laptops', 'Portable computers', 1),
        ('Smartphones', 'Mobile phones', 1),
        ('Clothing', 'Apparel and fashion', None),
        ('Men', 'Mens clothing', 4),
        ('Women', 'Womens clothing', 4),
        ('Home & Garden', 'Home improvement and garden', None),
        ('Sports', 'Sports equipment', None),
        ('Books', 'Books and literature', None),
        ('Toys', 'Toys and games', None)
    ]
    
    for name, desc, parent in categories_data:
        cursor.execute("""INSERT IGNORE INTO categories (name, description, parent_category_id) 
                         VALUES (%s, %s, %s)""", (name, desc, parent))
    
    # Suppliers
    for i in range(50):
        cursor.execute("""INSERT IGNORE INTO suppliers 
                         (name, country, contact_email, contact_phone, rating) 
                         VALUES (%s, %s, %s, %s, %s)""",
                      (fake.company(), fake.country(), fake.company_email(),
                       fake.phone_number(), round(random.uniform(3.5, 5.0), 2)))
    
    conn.commit()
    
    # Get IDs
    cursor.execute("SELECT category_id FROM categories")
    category_ids = [row[0] for row in cursor.fetchall()]
    cursor.execute("SELECT supplier_id FROM suppliers")
    supplier_ids = [row[0] for row in cursor.fetchall()]
    
    # Products
    brands = ['Apple', 'Samsung', 'Sony', 'Nike', 'Adidas', 'Dell', 'HP', 'LG', 'Canon', 'Logitech']
    for i in range(1000):
        cost = round(random.uniform(10, 800), 2)
        price = round(cost * random.uniform(1.3, 2.5), 2)
        cursor.execute("""INSERT IGNORE INTO products 
                         (name, sku, brand, category_id, supplier_id, price, cost, 
                          weight_kg, dimensions, rating_avg, total_reviews) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                      (fake.catch_phrase()[:50], f'SKU{10000+i}', random.choice(brands),
                       random.choice(category_ids), random.choice(supplier_ids),
                       price, cost, round(random.uniform(0.1, 10), 2),
                       f'{random.randint(10,50)}x{random.randint(10,50)}x{random.randint(5,30)}cm',
                       round(random.uniform(3.0, 5.0), 2), random.randint(0, 500)))
    
    conn.commit()
    cursor.execute("SELECT product_id FROM products")
    product_ids = [row[0] for row in cursor.fetchall()]
    
    # Customers
    for i in range(1000):
        reg_date = fake.date_between(start_date='-3y', end_date='-1m')
        cursor.execute("""INSERT IGNORE INTO customers 
                         (email, first_name, last_name, phone, age, gender, segment, 
                          loyalty_points, registration_date) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                      (fake.email(), fake.first_name(), fake.last_name(), 
                       fake.phone_number(), random.randint(18, 75),
                       random.choice(['Male', 'Female', 'Other']),
                       random.choice(['Premium', 'Standard', 'Basic']),
                       random.randint(0, 5000), reg_date))
    
    conn.commit()
    cursor.execute("SELECT customer_id FROM customers")
    customer_ids = [row[0] for row in cursor.fetchall()]
    
    # Shipping addresses
    for cid in customer_ids:
        num_addresses = random.randint(1, 3)
        for idx in range(num_addresses):
            cursor.execute("""INSERT INTO shipping_addresses 
                             (customer_id, address_type, street, city, state, country, 
                              zipcode, is_default, is_verified) 
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                          (cid, random.choice(['Home', 'Office', 'Other']),
                           fake.street_address(), fake.city(), fake.state(),
                           fake.country(), fake.zipcode(), 
                           idx == 0, random.choice([True, False])))
    
    conn.commit()
    
    # Orders
    order_data = []
    for i in range(2000):
        cid = random.choice(customer_ids)
        cursor.execute("SELECT address_id FROM shipping_addresses WHERE customer_id=%s", (cid,))
        addresses = cursor.fetchall()
        if not addresses:
            continue
        
        aid = random.choice(addresses)[0]
        order_date = fake.date_time_between(start_date='-1y', end_date='now')
        status = random.choice(['Completed', 'Pending', 'Shipped', 'Cancelled'])
        payment_status = 'Paid' if status in ['Completed', 'Shipped'] else random.choice(['Pending', 'Failed'])
        delivery_date = order_date + timedelta(days=random.randint(3, 14)) if status == 'Completed' else None
        
        cursor.execute("""INSERT INTO orders 
                         (customer_id, address_id, order_date, status, payment_status, 
                          delivery_date, total_amount, discount_amount, tax_amount, shipping_cost) 
                         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                      (cid, aid, order_date, status, payment_status, delivery_date,
                       0, 0, 0, round(random.uniform(5, 25), 2)))
        
        order_id = cursor.lastrowid
        order_data.append((order_id, cid, status))
        
        # Order items
        num_items = random.randint(1, 5)
        subtotal = 0
        for _ in range(num_items):
            pid = random.choice(product_ids)
            cursor.execute("SELECT price FROM products WHERE product_id=%s", (pid,))
            price = cursor.fetchone()[0]
            qty = random.randint(1, 5)
            discount = round(price * qty * random.uniform(0, 0.2), 2)
            
            cursor.execute("""INSERT INTO order_items 
                             (order_id, product_id, quantity, unit_price, discount) 
                             VALUES (%s, %s, %s, %s, %s)""",
                          (order_id, pid, qty, price, discount))
            subtotal += (price * qty - discount)
        
        tax = round(subtotal * 0.1, 2)
        cursor.execute("SELECT shipping_cost FROM orders WHERE order_id=%s", (order_id,))
        shipping = cursor.fetchone()[0]
        total = subtotal + tax + shipping
        
        cursor.execute("""UPDATE orders 
                         SET total_amount=%s, discount_amount=%s, tax_amount=%s 
                         WHERE order_id=%s""",
                      (total, subtotal * 0.1, tax, order_id))
        
        # Payment transaction
        cursor.execute("""INSERT INTO payment_transactions 
                         (order_id, payment_method, amount, status, transaction_reference) 
                         VALUES (%s, %s, %s, %s, %s)""",
                      (order_id, random.choice(['Credit Card', 'PayPal', 'Debit Card', 'Bank Transfer']),
                       total, payment_status, f'TXN{fake.uuid4()[:8]}'))
    
    conn.commit()
    
    # Reviews
    completed_orders = [(oid, cid) for oid, cid, status in order_data if status == 'Completed']
    for order_id, cid in random.sample(completed_orders, min(800, len(completed_orders))):
        cursor.execute("SELECT product_id FROM order_items WHERE order_id=%s", (order_id,))
        products = cursor.fetchall()
        for (pid,) in products:
            if random.random() < 0.4:
                rating = random.randint(1, 5)
                cursor.execute("""INSERT INTO reviews 
                                 (customer_id, product_id, order_id, rating, title, 
                                  comment, verified_purchase, helpful_count) 
                                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                              (cid, pid, order_id, rating, 
                               fake.sentence()[:50], fake.text(150),
                               True, random.randint(0, 100)))
    
    conn.commit()
    
    # Inventory
    for pid in product_ids:
        cursor.execute("""INSERT IGNORE INTO inventory 
                         (product_id, warehouse_location, warehouse_capacity, 
                          stock_quantity, reorder_level, last_restocked_date) 
                         VALUES (%s, %s, %s, %s, %s, %s)""",
                      (pid, f'WH{random.randint(1,5)}-{fake.city()[:20]}',
                       random.randint(500, 5000),
                       random.randint(0, 1000), random.randint(10, 100),
                       fake.date_between(start_date='-6m', end_date='today')))
    
    conn.commit()
    cursor.close()
    conn.close()
    print("✓ Generated all ecommerce data")

if __name__ == "__main__":
    generate_data()