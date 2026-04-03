import psycopg2
import json
import time
from datetime import datetime

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="inventory",
        user="admin",
        password="admin123"
    )

def get_pipeline_metrics():
    conn = get_db_connection()
    cur = conn.cursor()

    # Total events
    cur.execute("SELECT COUNT(*) FROM inventory_events")
    total_events = cur.fetchone()[0]

    # Total alerts
    cur.execute("SELECT COUNT(*) FROM restock_alerts")
    total_alerts = cur.fetchone()[0]

    # Events per category
    cur.execute("""
        SELECT category, COUNT(*) as count
        FROM inventory_events
        GROUP BY category
        ORDER BY count DESC
    """)
    category_counts = cur.fetchall()

    # Most critical products (stock = 0)
    cur.execute("""
        SELECT product_name, MIN(inventory_after) as stock
        FROM inventory_events
        WHERE restock_alert = true
        GROUP BY product_name
        ORDER BY stock ASC
        LIMIT 5
    """)
    critical_products = cur.fetchall()

    # Orders per status
    cur.execute("""
        SELECT order_status, COUNT(*) as count
        FROM inventory_events
        GROUP BY order_status
        ORDER BY count DESC
    """)
    status_counts = cur.fetchall()

    cur.close()
    conn.close()

    return {
        "total_events": total_events,
        "total_alerts": total_alerts,
        "category_counts": category_counts,
        "critical_products": critical_products,
        "status_counts": status_counts
    }

def print_dashboard(metrics):
    print("\033[2J\033[H")  # Clear screen
    print("=" * 60)
    print(f"  INVENTORY PIPELINE MONITOR")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    print(f"\n📊 PIPELINE STATUS")
    print(f"  Total events in DB  : {metrics['total_events']}")
    print(f"  Restock alerts      : {metrics['total_alerts']}")

    print(f"\n📦 EVENTS BY CATEGORY")
    for cat, count in metrics['category_counts']:
        bar = "█" * (count // 20)
        print(f"  {cat:<20} {count:>4} {bar}")

    print(f"\n🚨 CRITICAL STOCK (needs restock NOW)")
    for name, stock in metrics['critical_products']:
        status = "OUT OF STOCK" if stock == 0 else f"stock={stock}"
        print(f"  {name:<30} {status}")

    print(f"\n📋 ORDER STATUS BREAKDOWN")
    for status, count in metrics['status_counts']:
        print(f"  {status:<15} : {count} orders")

    print("\n" + "=" * 60)
    print("  Refreshing every 5 seconds... (Ctrl+C to stop)")
    print("=" * 60)

def run_monitor():
    print("Starting pipeline monitor...")
    try:
        while True:
            metrics = get_pipeline_metrics()
            print_dashboard(metrics)
            time.sleep(5)
    except KeyboardInterrupt:
        print("\nMonitor stopped.")

if __name__ == "__main__":
    run_monitor()