from generate_sales_data import generate_sales_data
from generate_customer_data import generate_customer_data
from generate_product_data import generate_product_data

if __name__ == "__main__":
    generate_sales_data(5000)
    generate_customer_data(3000)
    generate_product_data(2000)
    print("\n✓ All data generated successfully!")