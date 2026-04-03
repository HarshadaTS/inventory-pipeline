import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime

# Connect to PostgreSQL
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="inventory",
        user="admin",
        password="admin123"
    )

# Create tables if they don't exist
def setup_database():
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS inventory_events (
            id SERIAL PRIMARY KEY,
            event_id VARCHAR(100),
            order_id VARCHAR(50),
            product_id VARCHAR(20),
            product_name VARCHAR(100),
            category VARCHAR(50),
            warehouse_id VARCHAR(50),
            quantity INTEGER,
            total_price DECIMAL(10,2),
            order_status VARCHAR(20),
            inventory_after INTEGER,
            restock_alert BOOLEAN,
            restock_threshold INTEGER,
            event_timestamp TIMESTAMP,
            processed_at TIMESTAMP DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS restock_alerts (
            id SERIAL PRIMARY KEY,
            product_id VARCHAR(20),
            product_name VARCHAR(100),
            category VARCHAR(50),
            current_stock INTEGER,
            restock_threshold INTEGER,
            alert_time TIMESTAMP,
            warehouse_id VARCHAR(50)
        )
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("✅ Database tables ready")

def process_events():
    setup_database()

    consumer = KafkaConsumer(
        'inventory-events',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='inventory-processor'
    )

    print("✅ Connected to Kafka, waiting for events...")
    print("=" * 50)

    conn = get_db_connection()
    cur = conn.cursor()

    processed = 0
    alerts = 0

    for message in consumer:
        event = message.value

        # Save every event to inventory_events table
        cur.execute("""
            INSERT INTO inventory_events
            (event_id, order_id, product_id, product_name, category,
             warehouse_id, quantity, total_price, order_status,
             inventory_after, restock_alert, restock_threshold, event_timestamp)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            event['event_id'],
            event['order_id'],
            event['product_id'],
            event['product_name'],
            event['category'],
            event['warehouse_id'],
            event['quantity'],
            event['total_price'],
            event['order_status'],
            event['inventory_after'],
            event['restock_alert'],
            event['restock_threshold'],
            event['timestamp']
        ))

        # If restock alert, save to restock_alerts table
        if event['restock_alert']:
            cur.execute("""
                INSERT INTO restock_alerts
                (product_id, product_name, category, current_stock,
                 restock_threshold, alert_time, warehouse_id)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (
                event['product_id'],
                event['product_name'],
                event['category'],
                event['inventory_after'],
                event['restock_threshold'],
                event['timestamp'],
                event['warehouse_id']
            ))
            alerts += 1
            print(f"⚠️  RESTOCK ALERT saved: {event['product_name']} "
                  f"stock={event['inventory_after']}")

        conn.commit()
        processed += 1

        if processed % 100 == 0:
            print(f"✅ Processed {processed} events, {alerts} alerts saved to DB")

        if processed >= 1000:
            print(f"\n{'='*50}")
            print(f"✅ Done! Processed {processed} events")
            print(f"⚠️  Restock alerts in DB: {alerts}")
            print(f"{'='*50}")
            break

    cur.close()
    conn.close()

if __name__ == "__main__":
    process_events()