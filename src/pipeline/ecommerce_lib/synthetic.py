"""Deterministic synthetic ecommerce dataset with intentionally bad rows for data-quality testing.

Clean rows: 50 customers, 20 products, 300 orders.
Injected issues:
  customers: 2 newer duplicates of C001/C002 (untrimmed name, upper-case email) + C051 with a null email
  products:  P021 with a negative price
  orders:    O301 negative quantity, O302 null customer_id, O303 unknown status
"""
import random
from datetime import date, datetime, timedelta

from .schemas import CUSTOMERS_SCHEMA, ORDERS_SCHEMA, PRODUCTS_SCHEMA

SEED = 42
N_CUSTOMERS = 50
N_PRODUCTS = 20
N_ORDERS = 300

FIRST_NAMES = ["Aarav", "Diya", "Vivaan", "Ananya", "Ishaan", "Saanvi", "Kabir", "Meera", "Rohan", "Priya"]
LAST_NAMES = ["Sharma", "Patel", "Iyer", "Reddy", "Gupta", "Nair", "Singh", "Das", "Joshi", "Kulkarni"]
CITIES = [("Mumbai", "MH"), ("Pune", "MH"), ("Bengaluru", "KA"), ("Chennai", "TN"), ("Delhi", "DL"), ("Hyderabad", "TS")]
TIERS = ["Silver", "Gold", "Platinum"]
CATALOG = {
    "Electronics": ["Wireless Earbuds", "Smart Watch", "Power Bank", "Bluetooth Speaker", "USB-C Hub"],
    "Fashion": ["Running Shoes", "Denim Jacket", "Cotton Kurta", "Leather Wallet", "Sunglasses"],
    "Home": ["Coffee Maker", "Bedsheet Set", "Table Lamp", "Cookware Set", "Wall Clock"],
    "Beauty": ["Face Serum", "Hair Dryer", "Perfume", "Sunscreen", "Lip Balm Kit"],
}
BRANDS = ["Nova", "Zenith", "Aura", "Pulse", "Kosmo"]
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
STATUS_WEIGHTS = [15, 25, 45, 10, 5]
PAYMENTS = ["UPI", "CARD", "COD", "WALLET"]

# Row counts every layer must have after a pipeline run on this dataset.
EXPECTED_COUNTS = {
    "bronze_customers": N_CUSTOMERS + 3,
    "bronze_products": N_PRODUCTS + 1,
    "bronze_orders": N_ORDERS + 3,
    "silver_customers": N_CUSTOMERS,
    "silver_products": N_PRODUCTS,
    "silver_orders": N_ORDERS,
    "gold_customer_lifetime_value": N_CUSTOMERS,
    "gold_category_sales": len(CATALOG),
}


def customer_rows(seed: int = SEED) -> list:
    rng = random.Random(seed)
    rows = []
    for i in range(1, N_CUSTOMERS + 1):
        fn, ln = rng.choice(FIRST_NAMES), rng.choice(LAST_NAMES)
        city, state = rng.choice(CITIES)
        rows.append((
            f"C{i:03d}", fn, ln, f"{fn}.{ln}{i}@example.com".lower(), city, state,
            rng.choice(TIERS), date(2024, 1, 1) + timedelta(days=rng.randint(0, 700)),
        ))
    # Newer duplicates: silver must keep these (latest signup) after trimming / lower-casing.
    for dup in rows[:2]:
        rows.append((dup[0], f"  {dup[1]} ", dup[2], dup[3].upper(), dup[4], dup[5],
                     "Platinum", dup[7] + timedelta(days=30)))
    # Missing email: silver must drop it.
    rows.append((f"C{N_CUSTOMERS + 1:03d}", "No", "Email", None, "Pune", "MH", "Silver", date(2025, 1, 1)))
    return rows


def product_rows(seed: int = SEED) -> list:
    rng = random.Random(seed + 1)
    rows, i = [], 1
    for category, names in CATALOG.items():
        for name in names:
            rows.append((f"P{i:03d}", name, category, rng.choice(BRANDS), round(rng.uniform(5, 500), 2)))
            i += 1
    rows.append((f"P{i:03d}", "Broken Listing", "Home", "Nova", -5.0))  # negative price
    return rows


def order_rows(seed: int = SEED) -> list:
    rng = random.Random(seed + 2)
    prices = {p[0]: p[4] for p in product_rows(seed)[:N_PRODUCTS]}
    start = datetime(2026, 1, 1)
    rows = []
    for i in range(1, N_ORDERS + 1):
        pid = f"P{rng.randint(1, N_PRODUCTS):03d}"
        rows.append((
            f"O{i:04d}", f"C{rng.randint(1, N_CUSTOMERS):03d}", pid,
            start + timedelta(minutes=rng.randint(0, 180 * 24 * 60)),
            rng.randint(1, 5), prices[pid],
            rng.choices(STATUSES, weights=STATUS_WEIGHTS)[0], rng.choice(PAYMENTS),
        ))
    ts = datetime(2026, 3, 15, 10, 30)
    rows += [
        (f"O{N_ORDERS + 1:04d}", "C001", "P001", ts, -1, prices["P001"], "PLACED", "UPI"),      # bad quantity
        (f"O{N_ORDERS + 2:04d}", None, "P002", ts, 2, prices["P002"], "PLACED", "CARD"),         # null customer
        (f"O{N_ORDERS + 3:04d}", "C003", "P003", ts, 1, prices["P003"], "UNKNOWN", "COD"),     # bad status
    ]
    return rows


def generate_dataframes(spark, seed: int = SEED) -> dict:
    """Return the raw customers / products / orders DataFrames, typed with the schema contracts."""
    return {
        "customers": spark.createDataFrame(customer_rows(seed), CUSTOMERS_SCHEMA),
        "products": spark.createDataFrame(product_rows(seed), PRODUCTS_SCHEMA),
        "orders": spark.createDataFrame(order_rows(seed), ORDERS_SCHEMA),
    }
