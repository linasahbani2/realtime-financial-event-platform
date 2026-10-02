import json
import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Consumer

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

consumer = Consumer({
    'bootstrap.servers': os.environ["KAFKA_BOOTSTRAP"],
    'group.id': 'alerts-storage-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe(['financial.alerts'])

pg_conn = psycopg2.connect(
    host=os.environ["PG_HOST"],
    port=os.environ["PG_PORT"],
    dbname=os.environ["PG_DB"],
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"]
)
pg_conn.autocommit = True
cursor = pg_conn.cursor()

INSERT_QUERY = """
    INSERT INTO alerts (
        alert_type, event_id, customer_id, amount,
        recent_average, tx_count, event_timestamp
    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id, alert_type) DO NOTHING;
"""

if __name__ == "__main__":
    print("Demarrage du consumer d'alertes. Ctrl+C pour arreter.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erreur Kafka : {msg.error()}")
                continue

            alert = json.loads(msg.value())
            cursor.execute(INSERT_QUERY, (
                alert["alert_type"], alert["event_id"], alert["customer_id"],
                alert["amount"],
                alert.get("recent_average"),
                alert.get("tx_count"),
                alert["event_timestamp"]
            ))
            print(f"Alerte stockee : {alert['alert_type']} - client {alert['customer_id']}")

    except KeyboardInterrupt:
        print("\nArret demande.")
    finally:
        cursor.close()
        pg_conn.close()
        consumer.close()