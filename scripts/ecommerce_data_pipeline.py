#!/usr/bin/env python3
"""
E-commerce Synthetic Data Generator (Improved Version)
-----------------------------------------------------
Professional, safe, and production-like version.

Fixes:
- Removed Faker.unique crash risk
- UUID-based order IDs
- Safe CSV header writing
- Cleaner structure
"""

import argparse
import csv
import json
import random
import time
import uuid
from datetime import datetime
from pathlib import Path

from faker import Faker

# ---------------------------
# Configuration
# ---------------------------
DEFAULT_SEED = 42
DEFAULT_LOCALE = 'en_IN'

BATCH_CUSTOMERS_FILE = 'customers.csv'
BATCH_PRODUCTS_FILE = 'products.csv'
BATCH_ORDERS_FILE = 'orders.csv'
STREAM_ORDERS_FILE = 'orders_stream.csv'

fake = Faker(DEFAULT_LOCALE)

# ---------------------------
# Customer Generation (FIXED)
# ---------------------------
def generate_customers(num_customers=100, seed=None):
    if seed is not None:
        random.seed(seed)
        Faker.seed(seed)

    customers = []
    used_emails = set()

    for i in range(1, num_customers + 1):
        email = fake.email()

        # Ensure uniqueness manually
        while email in used_emails:
            email = fake.email()
        used_emails.add(email)

        customers.append({
            'customer_id': i,
            'name': fake.name(),
            'email': email,
            'phone': fake.phone_number(),
            'city': fake.city(),
            'state': fake.state(),
            'signup_date': fake.date_between(start_date='-2y', end_date='today').strftime('%Y-%m-%d')
        })

    return customers

# ---------------------------
# Product Generation
# ---------------------------
def generate_products(num_products=50, seed=None):
    if seed is not None:
        random.seed(seed)

    product_catalog = {
        'Electronics': ['iPhone 14', 'Samsung S23', 'OnePlus 11'],
        'Clothing': ['Nike Shoes', 'Adidas T-Shirt', 'Puma Hoodie'],
        'Books': ['Atomic Habits', 'Sapiens', 'Rich Dad Poor Dad'],
    }

    flat = [(cat, item) for cat, items in product_catalog.items() for item in items]

    if num_products > len(flat):
        chosen = [random.choice(flat) for _ in range(num_products)]
    else:
        chosen = random.sample(flat, num_products)

    products = []
    for i, (cat, name) in enumerate(chosen, start=1):
        products.append({
            'product_id': i,
            'product_name': name,
            'category': cat,
            'brand': name.split()[0],
            'price': round(random.uniform(100, 50000), 2),
            'stock_quantity': random.randint(5, 100)
        })

    return products

# ---------------------------
# Order Generation (UPDATED)
# ---------------------------
def generate_orders(num_orders, customers, products, seed=None):
    if seed is not None:
        random.seed(seed)

    orders = []

    for _ in range(num_orders):
        product = random.choice(products)
        quantity = random.randint(1, 5)

        orders.append({
            'order_id': str(uuid.uuid4()),  # FIXED
            'customer_id': random.choice(customers)['customer_id'],
            'product_id': product['product_id'],
            'quantity': quantity,
            'order_timestamp': fake.date_time_between(start_date='-6M',end_date='now').isoformat(),
            'payment_method': random.choice(['UPI', 'Card', 'COD']),
            'status': random.choice(['Completed', 'Pending', 'Cancelled']),
            'shipping_city': fake.city(),
            'order_value': round(product['price'] * quantity, 2)
        })

    return orders

# ---------------------------
# CSV Writing (FIXED)
# ---------------------------
def save_to_csv(data, filename, fieldnames, mode='w'):
    Path(filename).parent.mkdir(parents=True, exist_ok=True)

    write_header = (mode == 'w' or not Path(filename).exists())

    with open(filename, mode, newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        if write_header:
            writer.writeheader()

        writer.writerows(data)

    print(f"Saved {len(data)} records → {filename}")

# ---------------------------
# Streaming Mode
# ---------------------------
def stream_orders(customers, products, frequency=2):
    print("Streaming started... Press Ctrl+C to stop.")

    try:
        while True:
            order = generate_orders(1, customers, products)[0]

            save_to_csv(
                [order],
                STREAM_ORDERS_FILE,
                list(order.keys()),
                mode='a'
            )

            time.sleep(frequency)

    except KeyboardInterrupt:
        print("Streaming stopped.")

# ---------------------------
# Main
# ---------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=['batch', 'stream'], default='batch')
    parser.add_argument('--num_customers', type=int, default=50)
    parser.add_argument('--num_products', type=int, default=20)
    parser.add_argument('--num_orders', type=int, default=100)
    parser.add_argument('--frequency', type=int, default=2)

    args = parser.parse_args()

    customers = generate_customers(args.num_customers)
    products = generate_products(args.num_products)

    if args.mode == 'batch':
        orders = generate_orders(args.num_orders, customers, products)

        save_to_csv(customers, BATCH_CUSTOMERS_FILE, customers[0].keys())
        save_to_csv(products, BATCH_PRODUCTS_FILE, products[0].keys())
        save_to_csv(orders, BATCH_ORDERS_FILE, orders[0].keys())

        print("\nBatch generation completed!")

    elif args.mode == 'stream':
        stream_orders(customers, products, args.frequency)


if __name__ == "__main__":
    main()