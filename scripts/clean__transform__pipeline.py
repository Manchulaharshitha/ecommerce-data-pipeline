#!/usr/bin/env python3
"""
clean_transform_pipeline.py (PRO VERSION)

Improved:
- UUID-safe order_id
- Faster vectorized calculations
- Safe phone handling
- Cleaner + production-ready
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
import re
from decimal import Decimal, ROUND_HALF_UP
import warnings

warnings.filterwarnings("ignore", category=FutureWarning)

# Optional phone library
try:
    import phonenumbers
    PHONE_LIB = True
except:
    PHONE_LIB = False

# ---------- CONFIG ----------
CUSTOMERS_CSV = "customers.csv"
PRODUCTS_CSV = "products.csv"
ORDERS_CSV = "orders.csv"

OUT_CUSTOMERS = "clean_customers.csv"
OUT_PRODUCTS = "clean_products.csv"
OUT_ORDERS = "clean_orders.csv"

# ---------- UTILS ----------
def safe_read_csv(path):
    return pd.read_csv(path, dtype=str, keep_default_na=False)

def normalize_email(email):
    if not email:
        return np.nan
    return re.sub(r"\s+", "", str(email).lower())

def normalize_phone(phone):
    if not phone:
        return np.nan

    cleaned = re.sub(r"[^\d\+]", "", str(phone))

    if PHONE_LIB:
        try:
            parsed = phonenumbers.parse(cleaned, "IN")
            if phonenumbers.is_valid_number(parsed):
                return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
        except:
            pass

    digits = re.sub(r"\D", "", cleaned)
    return digits if len(digits) >= 8 else np.nan

def parse_date_safe(val):
    try:
        return pd.to_datetime(val, utc=True, errors="coerce")
    except:
        return pd.NaT

def money_round(x):
    try:
        return float(Decimal(str(x)).quantize(Decimal("1.00"), rounding=ROUND_HALF_UP))
    except:
        return np.nan

# ---------- CLEAN CUSTOMERS ----------
def clean_customers(df):
    df = df.copy()

    df['customer_id'] = pd.to_numeric(df.get('customer_id'), errors='coerce').astype('Int64')
    df['name'] = df.get('name').str.strip()
    df['email'] = df.get('email').apply(normalize_email)
    df['phone'] = df.get('phone').apply(normalize_phone)

    df['city'] = df.get('city').str.title()
    df['state'] = df.get('state').str.title()

    df['signup_date'] = df.get('signup_date').apply(parse_date_safe)

    df = df.dropna(subset=['customer_id', 'name'])

    df = df.drop_duplicates(subset=['email'], keep='first')

    return df.reset_index(drop=True)

# ---------- CLEAN PRODUCTS ----------
def clean_products(df):
    df = df.copy()

    df['product_id'] = pd.to_numeric(df.get('product_id'), errors='coerce').astype('Int64')
    df['product_name'] = df.get('product_name').str.strip()
    df['category'] = df.get('category').str.title()
    df['brand'] = df.get('brand').str.title()

    df['price'] = pd.to_numeric(df.get('price'), errors='coerce').apply(money_round)
    df['stock_quantity'] = pd.to_numeric(df.get('stock_quantity'), errors='coerce').astype('Int64')

    df = df.dropna(subset=['product_id', 'product_name'])
    df = df.drop_duplicates(subset=['product_id'])

    return df.reset_index(drop=True)

# ---------- CLEAN ORDERS (FIXED) ----------
def clean_orders(df, valid_customers, valid_products, price_map):
    df = df.copy()

    # ✅ FIX: Keep order_id as string (supports UUID)
    df['order_id'] = df.get('order_id').astype(str)

    df['customer_id'] = pd.to_numeric(df.get('customer_id'), errors='coerce').astype('Int64')
    df['product_id'] = pd.to_numeric(df.get('product_id'), errors='coerce').astype('Int64')

    df['quantity'] = pd.to_numeric(df.get('quantity'), errors='coerce').fillna(1).astype(int)

    df['order_timestamp'] = df.get('order_timestamp').apply(parse_date_safe)

    df['payment_method'] = df.get('payment_method').str.title()
    df['status'] = df.get('status').str.title()
    df['shipping_city'] = df.get('shipping_city').str.title()

    # Remove invalid references
    df = df[df['customer_id'].isin(valid_customers)]
    df = df[df['product_id'].isin(valid_products)]

    # ✅ FIX: Vectorized price calculation
    df['price'] = df['product_id'].map(price_map)
    df['order_value'] = (df['price'] * df['quantity']).apply(money_round)

    df = df.drop(columns=['price'])

    df['order_timestamp'] = df['order_timestamp'].fillna(
        pd.to_datetime(datetime.now(timezone.utc))
    )

    df = df.drop_duplicates(subset=['order_id'])

    return df.reset_index(drop=True)

# ---------- ANALYTICS ----------
def analytics(cust, prod, ords):
    print("\n=== ANALYTICS ===")
    print(f"Customers: {len(cust)}")
    print(f"Products: {len(prod)}")
    print(f"Orders: {len(ords)}")

    top = (
        ords.groupby('product_id')['order_value']
        .sum()
        .reset_index()
        .sort_values(by='order_value', ascending=False)
        .head(5)
    )

    top = top.merge(prod[['product_id', 'product_name']], on='product_id')
    print("\nTop Products:")
    print(top)

# ---------- MAIN ----------
def main():
    print("Loading data...")

    cust = clean_customers(safe_read_csv(CUSTOMERS_CSV))
    prod = clean_products(safe_read_csv(PRODUCTS_CSV))

    price_map = dict(zip(prod['product_id'], prod['price']))

    ords = clean_orders(
        safe_read_csv(ORDERS_CSV),
        set(cust['customer_id']),
        set(prod['product_id']),
        price_map
    )

    cust.to_csv(OUT_CUSTOMERS, index=False)
    prod.to_csv(OUT_PRODUCTS, index=False)
    ords.to_csv(OUT_ORDERS, index=False)

    analytics(cust, prod, ords)

    print("\n✅ Pipeline completed successfully")

if __name__ == "__main__":
    main()
