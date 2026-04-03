import json
import time
from kafka import KafkaProducer

def create_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',
        retries=3
    )

def send_orders(mode='normal'):
    producer = create_producer()

    # Load the generated orders
    with open('../data-generator/orders_stream.jsonl', 'r') as f:
        orders = [json.loads(line) for line in f]

    print(f"Loaded {len(orders)} orders from file")
    print(f"Mode: {mode.upper()}")
    print(f"Sending to Kafka topic: inventory-events\n")

    sent = 0
    alerts = 0

    for order in orders:
        producer.send('inventory-events', value=order)
        sent += 1

        if order['restock_alert']:
            alerts += 1
            print(f"[{sent}/1000] ⚠️  RESTOCK ALERT: {order['product_name']} "
                  f"stock={order['inventory_after']} "
                  f"threshold={order['restock_threshold']}")
        else:
            if sent % 50 == 0:
                print(f"[{sent}/1000] ✅ Sent: {order['product_name']} "
                      f"stock={order['inventory_after']}")

        # Normal mode = slow, burst mode = fast
        if mode == 'normal':
            time.sleep(0.1)
        else:
            time.sleep(0.001)

    producer.flush()
    print(f"\n{'='*50}")
    print(f"✅ Done! Sent {sent} orders to Kafka")
    print(f"⚠️  Restock alerts sent: {alerts}")
    print(f"{'='*50}")

if __name__ == "__main__":
    import sys
    mode = sys.argv[1] if len(sys.argv) > 1 else 'normal'
    send_orders(mode)