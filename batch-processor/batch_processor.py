import json
import psycopg2
import os
from datetime import datetime

def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        database="inventory",
        user="admin",
        password="admin123"
    )

def run_batch():
    print("=" * 60)
    print(f"BATCH JOB STARTED: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    conn = get_db_connection()
    cur = conn.cursor()

    # ── Query 1: Products that need restocking ────────────────
    print("\n📦 RESTOCK FORECAST REPORT")
    print("-" * 40)
    cur.execute("""
        SELECT
            product_name,
            category,
            MIN(inventory_after) as current_stock,
            restock_threshold,
            COUNT(*) as total_orders,
            SUM(quantity) as total_units_sold
        FROM inventory_events
        WHERE restock_alert = true
        GROUP BY product_name, category, restock_threshold
        ORDER BY current_stock ASC
    """)
    restock_products = cur.fetchall()

    restock_report = []
    for row in restock_products:
        name, cat, stock, threshold, orders, sold = row
        restock_qty = max(100, sold * 2)
        print(f"  {name} ({cat})")
        print(f"    Stock: {stock} | Threshold: {threshold} | "
              f"Sold: {sold} | Restock: {restock_qty} units")
        restock_report.append({
            "product_name": name,
            "category": cat,
            "current_stock": stock,
            "restock_threshold": threshold,
            "total_units_sold": sold,
            "recommended_restock_qty": restock_qty
        })

    # ── Query 2: Sales by category ────────────────────────────
    print("\n📊 SALES BY CATEGORY")
    print("-" * 40)
    cur.execute("""
        SELECT
            category,
            COUNT(*) as total_orders,
            SUM(quantity) as units_sold,
            ROUND(SUM(total_price)::numeric, 2) as revenue
        FROM inventory_events
        WHERE order_status != 'cancelled'
        GROUP BY category
        ORDER BY revenue DESC
    """)
    category_sales = cur.fetchall()

    category_report = []
    for row in category_sales:
        cat, orders, units, revenue = row
        print(f"  {cat}: {orders} orders | "
              f"{units} units | ${revenue} revenue")
        category_report.append({
            "category": cat,
            "total_orders": orders,
            "units_sold": units,
            "revenue": float(revenue)
        })

    # ── Query 3: Pipeline metrics ─────────────────────────────
    print("\n⚡ PIPELINE METRICS")
    print("-" * 40)
    cur.execute("""
        SELECT
            COUNT(*) as total_events,
            COUNT(CASE WHEN restock_alert THEN 1 END) as total_alerts
        FROM inventory_events
    """)
    metrics = cur.fetchone()
    total, alerts = metrics
    print(f"  Total events processed : {total}")
    print(f"  Restock alerts fired   : {alerts}")

    # ── Query 4: Top 5 most sold products ─────────────────────
    print("\n🏆 TOP 5 MOST SOLD PRODUCTS")
    print("-" * 40)
    cur.execute("""
        SELECT
            product_name,
            SUM(quantity) as total_sold,
            ROUND(SUM(total_price)::numeric, 2) as total_revenue
        FROM inventory_events
        WHERE order_status != 'cancelled'
        GROUP BY product_name
        ORDER BY total_sold DESC
        LIMIT 5
    """)
    top_products = cur.fetchall()

    top_report = []
    for row in top_products:
        name, sold, revenue = row
        print(f"  {name}: {sold} units sold | ${revenue} revenue")
        top_report.append({
            "product_name": name,
            "total_sold": sold,
            "total_revenue": float(revenue)
        })

    # ── Save results to cold storage ──────────────────────────
    os.makedirs("../storage/cold", exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    report = {
        "batch_run_time": datetime.now().isoformat(),
        "pipeline_metrics": {
            "total_events": total,
            "total_alerts": alerts
        },
        "restock_forecast": restock_report,
        "category_sales": category_report,
        "top_products": top_report
    }

    filename = f"../storage/cold/batch_report_{timestamp}.json"
    with open(filename, "w") as f:
        json.dump(report, f, indent=2)

    print(f"\n💾 Report saved to: {filename}")
    print("\n" + "=" * 60)
    print("BATCH JOB COMPLETED SUCCESSFULLY")
    print("=" * 60)

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_batch()