import json
import random
import uuid
from datetime import datetime, timedelta

# ── Product catalogue ────────────────────────────────────────────────────────
PRODUCTS = [
    {"id": "PROD-001", "name": "Wireless Headphones",   "category": "Electronics",  "price": 49.99,  "restock_threshold": 20},
    {"id": "PROD-002", "name": "USB-C Hub",             "category": "Electronics",  "price": 29.99,  "restock_threshold": 15},
    {"id": "PROD-003", "name": "Mechanical Keyboard",   "category": "Electronics",  "price": 89.99,  "restock_threshold": 10},
    {"id": "PROD-004", "name": "Yoga Mat",              "category": "Sports",       "price": 24.99,  "restock_threshold": 25},
    {"id": "PROD-005", "name": "Resistance Bands",      "category": "Sports",       "price": 14.99,  "restock_threshold": 30},
    {"id": "PROD-006", "name": "Running Shoes",         "category": "Sports",       "price": 79.99,  "restock_threshold": 12},
    {"id": "PROD-007", "name": "Cotton T-Shirt",        "category": "Clothing",     "price": 19.99,  "restock_threshold": 40},
    {"id": "PROD-008", "name": "Denim Jeans",           "category": "Clothing",     "price": 54.99,  "restock_threshold": 20},
    {"id": "PROD-009", "name": "Winter Jacket",         "category": "Clothing",     "price": 129.99, "restock_threshold": 8},
    {"id": "PROD-010", "name": "Blender",               "category": "Kitchen",      "price": 39.99,  "restock_threshold": 15},
    {"id": "PROD-011", "name": "Air Fryer",             "category": "Kitchen",      "price": 69.99,  "restock_threshold": 10},
    {"id": "PROD-012", "name": "Coffee Maker",          "category": "Kitchen",      "price": 44.99,  "restock_threshold": 12},
    {"id": "PROD-013", "name": "Novel: The Last Code",  "category": "Books",        "price": 12.99,  "restock_threshold": 50},
    {"id": "PROD-014", "name": "Python Programming",    "category": "Books",        "price": 34.99,  "restock_threshold": 30},
    {"id": "PROD-015", "name": "Face Moisturiser",      "category": "Beauty",       "price": 22.99,  "restock_threshold": 35},
]

WAREHOUSES = ["WH-MUMBAI-01", "WH-DELHI-02", "WH-BANGALORE-03", "WH-HYDERABAD-04"]

STATUSES = ["placed", "confirmed", "shipped", "delivered", "cancelled"]

# Starting inventory per product (simulate a live store)
inventory = {p["id"]: random.randint(50, 200) for p in PRODUCTS}


def generate_event(order_num: int, base_time: datetime) -> dict:
    product     = random.choice(PRODUCTS)
    pid         = product["id"]
    qty         = random.randint(1, 5)
    status      = random.choices(
        STATUSES,
        weights=[40, 30, 15, 10, 5],   # most orders are placed/confirmed
        k=1
    )[0]

    inv_before  = inventory[pid]
    # Only reduce inventory for placed/confirmed/shipped orders
    if status not in ("cancelled",) and inv_before >= qty:
        inventory[pid] = max(0, inv_before - qty)
    inv_after   = inventory[pid]

    # Simulate bursty traffic: 20 % chance of flash-sale spike timestamp
    if random.random() < 0.20:
        jitter = timedelta(seconds=random.randint(0, 10))   # orders cluster
    else:
        jitter = timedelta(seconds=random.randint(0, 300))

    event_time  = base_time + jitter

    return {
        "event_id":          str(uuid.uuid4()),
        "order_id":          f"ORD-{order_num:05d}",
        "product_id":        pid,
        "product_name":      product["name"],
        "category":          product["category"],
        "warehouse_id":      random.choice(WAREHOUSES),
        "quantity":          qty,
        "unit_price":        product["price"],
        "total_price":       round(product["price"] * qty, 2),
        "order_status":      status,
        "inventory_before":  inv_before,
        "inventory_after":   inv_after,
        "restock_threshold": product["restock_threshold"],
        "restock_alert":     inv_after <= product["restock_threshold"],
        "timestamp":         event_time.isoformat(),
        "processing_delay_ms": random.randint(10, 500),   # simulated latency
    }


def main():
    NUM_EVENTS  = 1000
    START_TIME  = datetime(2025, 4, 1, 9, 0, 0)   # 9 AM on April 1 2025
    events      = []

    current_time = START_TIME
    for i in range(1, NUM_EVENTS + 1):
        event = generate_event(i, current_time)
        events.append(event)
        # Advance clock by 30–120 seconds between orders on average
        current_time += timedelta(seconds=random.randint(30, 120))

    # ── Save as JSON Lines (best for streaming pipelines) ────────────────────
    with open("orders_stream.jsonl", "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    # ── Save as JSON array (easy for inspection) ─────────────────────────────
    with open("orders_batch.json", "w") as f:
        json.dump(events, f, indent=2)

    # ── Quick summary ─────────────────────────────────────────────────────────
    restock_alerts = sum(1 for e in events if e["restock_alert"])
    cancelled      = sum(1 for e in events if e["order_status"] == "cancelled")

    print(f"✅  Generated {NUM_EVENTS} order events")
    print(f"📦  Restock alerts triggered : {restock_alerts}")
    print(f"❌  Cancelled orders         : {cancelled}")
    print(f"💾  Files saved:")
    print(f"    → orders_stream.jsonl  (one JSON object per line — for Kafka/Kinesis)")
    print(f"    → orders_batch.json    (full array — for batch inspection)")
    print(f"\nSample event:")
    print(json.dumps(events[0], indent=2))


if __name__ == "__main__":
    main()
