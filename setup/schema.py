import mysql.connector
from config.settings import MYSQL_CONFIG

def create_tables():
    conn = mysql.connector.connect(
        host=MYSQL_CONFIG['host'],
        user=MYSQL_CONFIG['user'],
        password=MYSQL_CONFIG['password'],
        port=MYSQL_CONFIG['port']
    )
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE ecommerce")
    cursor.execute("CREATE DATABASE IF NOT EXISTS ecommerce")
    cursor.execute("USE ecommerce")
    
    tables = {
        'categories': """
            CREATE TABLE IF NOT EXISTS categories (
                category_id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) UNIQUE NOT NULL,
                description TEXT,
                parent_category_id INT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        'suppliers': """
            CREATE TABLE IF NOT EXISTS suppliers (
                supplier_id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) UNIQUE NOT NULL,
                country VARCHAR(100),
                contact_email VARCHAR(100),
                contact_phone VARCHAR(20),
                rating DECIMAL(3,2),
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        'products': """
            CREATE TABLE IF NOT EXISTS products (
                product_id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(200) NOT NULL,
                sku VARCHAR(50) UNIQUE NOT NULL,
                brand VARCHAR(100),
                category_id INT,
                supplier_id INT,
                price DECIMAL(10,2),
                cost DECIMAL(10,2),
                weight_kg DECIMAL(8,2),
                dimensions VARCHAR(50),
                is_active BOOLEAN DEFAULT TRUE,
                rating_avg DECIMAL(3,2),
                total_reviews INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                FOREIGN KEY (category_id) REFERENCES categories(category_id),
                FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
            )
        """,
        'customers': """
            CREATE TABLE IF NOT EXISTS customers (
                customer_id INT PRIMARY KEY AUTO_INCREMENT,
                email VARCHAR(100) UNIQUE NOT NULL,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                phone VARCHAR(20),
                age INT,
                gender VARCHAR(10),
                segment VARCHAR(20),
                loyalty_points INT DEFAULT 0,
                account_status VARCHAR(20) DEFAULT 'Active',
                registration_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """,
        'shipping_addresses': """
            CREATE TABLE IF NOT EXISTS shipping_addresses (
                address_id INT PRIMARY KEY AUTO_INCREMENT,
                customer_id INT,
                address_type VARCHAR(20),
                street VARCHAR(200),
                city VARCHAR(100),
                state VARCHAR(50),
                country VARCHAR(100),
                zipcode VARCHAR(20),
                is_default BOOLEAN DEFAULT FALSE,
                is_verified BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
            )
        """,
        'orders': """
            CREATE TABLE IF NOT EXISTS orders (
                order_id INT PRIMARY KEY AUTO_INCREMENT,
                customer_id INT,
                address_id INT,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20),
                payment_status VARCHAR(20),
                delivery_date TIMESTAMP NULL,
                total_amount DECIMAL(10,2),
                discount_amount DECIMAL(10,2) DEFAULT 0,
                tax_amount DECIMAL(10,2) DEFAULT 0,
                shipping_cost DECIMAL(10,2) DEFAULT 0,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
                FOREIGN KEY (address_id) REFERENCES shipping_addresses(address_id)
            )
        """,
        'order_items': """
            CREATE TABLE IF NOT EXISTS order_items (
                order_item_id INT PRIMARY KEY AUTO_INCREMENT,
                order_id INT,
                product_id INT,
                quantity INT,
                unit_price DECIMAL(10,2),
                discount DECIMAL(10,2) DEFAULT 0,
                FOREIGN KEY (order_id) REFERENCES orders(order_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        """,
        'payment_transactions': """
            CREATE TABLE IF NOT EXISTS payment_transactions (
                transaction_id INT PRIMARY KEY AUTO_INCREMENT,
                order_id INT,
                payment_method VARCHAR(50),
                amount DECIMAL(10,2),
                transaction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20),
                transaction_reference VARCHAR(100),
                FOREIGN KEY (order_id) REFERENCES orders(order_id)
            )
        """,
        'reviews': """
            CREATE TABLE IF NOT EXISTS reviews (
                review_id INT PRIMARY KEY AUTO_INCREMENT,
                customer_id INT,
                product_id INT,
                order_id INT,
                rating INT CHECK (rating BETWEEN 1 AND 5),
                title VARCHAR(200),
                comment TEXT,
                verified_purchase BOOLEAN DEFAULT TRUE,
                helpful_count INT DEFAULT 0,
                review_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id),
                FOREIGN KEY (order_id) REFERENCES orders(order_id)
            )
        """,
        'inventory': """
            CREATE TABLE IF NOT EXISTS inventory (
                inventory_id INT PRIMARY KEY AUTO_INCREMENT,
                product_id INT UNIQUE,
                warehouse_location VARCHAR(100),
                warehouse_capacity INT,
                stock_quantity INT,
                reorder_level INT,
                last_restocked_date DATE,
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        """
    }
    
    for table_name, query in tables.items():
        cursor.execute(query)
        print(f"✓ Created table: {table_name}")
    
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    create_tables()