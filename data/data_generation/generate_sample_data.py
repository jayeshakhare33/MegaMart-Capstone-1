"""
MegaMart Sample Data Generator
================================
Generates realistic sample CSV files with intentional data quality issues
for the capstone project.

This script creates sample data that students will use to practice
data cleaning, validation, and ETL processes.
"""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)


# Configuration
STORES = [
    {"id": 1, "name": "HITEC City Store", "location": "HITEC City"},
    {"id": 2, "name": "Banjara Hills Store", "location": "Banjara Hills"},
    {"id": 3, "name": "Ameerpet Store", "location": "Ameerpet"},
    {"id": 4, "name": "Kukatpally Store", "location": "Kukatpally"},
    {"id": 5, "name": "Dilsukhnagar Store", "location": "Dilsukhnagar"}
]

MANAGERS = ["Rajesh Kumar", "Priya Sharma", "Amit Patel", None, "Venkat Reddy"]

CATEGORIES = [
    "Groceries", "Dairy", "Cooking Oil", "Spices", "Beverages",
    "Snacks", "Frozen Foods", "Personal Care", "Cleaning", "Toiletries",
    "Electronics", "Home & Kitchen"
]

PRODUCTS = [
    ("Basmati Rice 5kg", "Groceries", 450.00),
    ("Wheat Flour 1kg", "Groceries", 35.00),
    ("Amul Milk 1L", "Dairy", 55.00),
    ("Paneer 250g", "Dairy", 180.00),
    ("Refined Oil 1L", "Cooking Oil", 185.00),
    ("Mustard Oil 500ml", "Cooking Oil", 120.00),
    ("Turmeric Powder 100g", "Spices", 45.00),
    ("Chili Powder 100g", "Spices", 65.00),
    ("Coca Cola 1.5L", "Beverages", 85.00),
    ("Mineral Water 1L", "Beverages", 25.00),
    ("Lays Chips 40g", "Snacks", 30.00),
    ("Biscuits Mix 200g", "Snacks", 40.00),
    ("Shampoo 200ml", "Personal Care", 120.00),
    ("Soap 125g", "Personal Care", 35.00),
]

SUPPLIERS = [
    ("ABC Wholesale Pvt Ltd", "Arjun Singh", "9000000001"),
    ("XYZ Foods Distribution", "Meera Nair", "9000000002"),
    ("Fresh Produce Co", "Suresh Kumar", "9000000003"),
    ("Dairy Supplies International", "Kavya Sharma", "9000000004"),
    ("Premium Imports Ltd", "Rohit Verma", "9000000005"),
]


def generate_stores_csv(output_path):
    """Generate stores.csv with intentional issues"""
    print("Generating stores.csv...")
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['store_id', 'store_name', 'location', 'manager_name'])
        writer.writeheader()
        
        for i, store in enumerate(STORES):
            writer.writerow({
                'store_id': store['id'],
                'store_name': store['name'],
                'location': store['location'],
                'manager_name': MANAGERS[i]  # One is intentionally None
            })
    
    print(f"✓ Created stores.csv ({len(STORES)} records)")


def generate_products_csv(output_path):
    """Generate products.csv"""
    print("Generating products.csv...")
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['product_id', 'product_name', 'category', 'unit_price', 'supplier_id'])
        writer.writeheader()
        
        for idx, (name, category, price) in enumerate(PRODUCTS, start=1001):
            supplier_id = random.randint(1, len(SUPPLIERS))
            
            # Introduce occasional data quality issues
            if idx == 1005:  # Invalid price
                price = -100.00
            elif idx == 1007:  # Zero price
                price = 0.00
            
            writer.writerow({
                'product_id': idx,
                'product_name': name,
                'category': category,
                'unit_price': price,
                'supplier_id': supplier_id
            })
    
    print(f"✓ Created products.csv ({len(PRODUCTS)} records with issues)")


def generate_suppliers_csv(output_path):
    """Generate suppliers.csv"""
    print("Generating suppliers.csv...")
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['supplier_id', 'supplier_name', 'contact_person', 'phone'])
        writer.writeheader()
        
        for idx, (name, person, phone) in enumerate(SUPPLIERS, start=1):
            # Introduce format inconsistencies
            if idx == 2:
                phone = "+91-" + phone  # Different format
            elif idx == 4:
                phone = "91" + phone  # Another format
            
            writer.writerow({
                'supplier_id': idx,
                'supplier_name': name,
                'contact_person': person,
                'phone': phone
            })
    
    print(f"✓ Created suppliers.csv ({len(SUPPLIERS)} records)")


def generate_sales_transactions_csv(output_path, num_records=2000):
    """Generate sales_transactions.csv with realistic data"""
    print(f"Generating sales_transactions.csv ({num_records} records)...")
    
    start_date = datetime(2024, 1, 1)
    duplicate_count = 0
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'transaction_id', 'store_id', 'product_id', 'quantity_sold', 'unit_price', 'total_amount', 'sale_date'
        ])
        writer.writeheader()
        
        for t_id in range(50001, 50001 + num_records):
            store_id = random.randint(1, 5)
            product = random.choice(PRODUCTS)
            product_id = 1001 + PRODUCTS.index(product)
            quantity = random.randint(1, 10)
            
            # Random date within 2024
            days_offset = random.randint(0, 364)
            sale_date = start_date + timedelta(days=days_offset)
            
            unit_price = product[2]
            total_amount = quantity * unit_price
            
            writer.writerow({
                'transaction_id': t_id,
                'store_id': store_id,
                'product_id': product_id,
                'quantity_sold': quantity,
                'unit_price': unit_price,
                'total_amount': total_amount,
                'sale_date': sale_date.strftime('%Y-%m-%d')
            })
            
            # Add duplicate record for some transactions (data quality issue)
            if t_id % 500 == 0 and duplicate_count < 5:
                writer.writerow({
                    'transaction_id': t_id + 10000,  # Different ID, same data
                    'store_id': store_id,
                    'product_id': product_id,
                    'quantity_sold': quantity,
                    'unit_price': unit_price,
                    'total_amount': total_amount,
                    'sale_date': sale_date.strftime('%Y-%m-%d')
                })
                duplicate_count += 1
    
    print(f"✓ Created sales_transactions.csv ({num_records} records + {duplicate_count} intentional duplicates)")


def generate_inventory_csv(output_path):
    """Generate inventory.csv"""
    print("Generating inventory.csv...")
    
    records_count = 0
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['inventory_id', 'store_id', 'product_id', 'quantity_on_hand'])
        writer.writeheader()
        
        inv_id = 1
        for store_id in range(1, 6):
            for product_id in range(1001, 1001 + len(PRODUCTS)):
                quantity = random.randint(0, 500)
                
                # Introduce some out-of-stock situations
                if random.random() < 0.1:  # 10% chance
                    quantity = 0
                
                writer.writerow({
                    'inventory_id': inv_id,
                    'store_id': store_id,
                    'product_id': product_id,
                    'quantity_on_hand': quantity
                })
                inv_id += 1
                records_count += 1
    
    print(f"✓ Created inventory.csv ({records_count} records)")


def generate_purchase_orders_csv(output_path, num_orders=300):
    """Generate purchase_orders.csv"""
    print(f"Generating purchase_orders.csv ({num_orders} records)...")
    
    start_date = datetime(2024, 1, 1)
    
    with open(output_path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'po_id', 'supplier_id', 'order_date', 'delivery_date'
        ])
        writer.writeheader()
        
        for po_id in range(70001, 70001 + num_orders):
            supplier_id = random.randint(1, len(SUPPLIERS))
            
            days_offset = random.randint(0, 364)
            order_date = start_date + timedelta(days=days_offset)
            
            # Most orders delivered, some pending (no delivery date)
            if random.random() < 0.15:  # 15% pending
                delivery_date = ""
            else:
                delivery_days = random.randint(3, 14)
                delivery_date = (order_date + timedelta(days=delivery_days)).strftime('%Y-%m-%d')
            
            writer.writerow({
                'po_id': po_id,
                'supplier_id': supplier_id,
                'order_date': order_date.strftime('%Y-%m-%d'),
                'delivery_date': delivery_date
            })
    
    print(f"✓ Created purchase_orders.csv ({num_orders} records)")


def main():
    """Generate all sample CSV files"""
    print("\n" + "="*50)
    print("MegaMart Sample Data Generator")
    print("="*50 + "\n")
    
    # Create output directory
    output_dir = Path('data/raw')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate all CSV files
    generate_stores_csv(output_dir / 'stores.csv')
    generate_products_csv(output_dir / 'products.csv')
    generate_suppliers_csv(output_dir / 'suppliers.csv')
    generate_sales_transactions_csv(output_dir / 'sales_transactions.csv', num_records=2000)
    generate_inventory_csv(output_dir / 'inventory.csv')
    generate_purchase_orders_csv(output_dir / 'purchase_orders.csv', num_orders=300)
    
    print("\n" + "="*50)
    print("✓ Sample data generation complete!")
    print("="*50)
    print("\nGenerated files:")
    for csv_file in sorted(output_dir.glob('*.csv')):
        size = csv_file.stat().st_size
        print(f"  • {csv_file.name:35s} ({size:,} bytes)")
    
    print("\nData Quality Issues Included:")
    print("  • Duplicate sales transactions (5 duplicates)")
    print("  • Missing manager name in stores")
    print("  • Invalid prices (-100, 0)")
    print("  • Inconsistent phone number formats")
    print("  • Out-of-stock situations (0 quantity)")
    print("  • Pending purchase orders (NULL delivery dates)")
    
    print("\nNext Steps:")
    print("  1. Review the generated CSV files")
    print("  2. Implement data validation in Python")
    print("  3. Load data into the database")
    print("  4. Check data_loading.log for issues found")


if __name__ == '__main__':
    main()
