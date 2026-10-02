import json
import os
import psycopg2
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Consumer

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

kafka_conf = {
    'bootstrap.servers': os.environ["KAFKA_BOOTSTRAP"],
    'group.id': 'transactions-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(kafka_conf)
consumer.subscribe(['financial.transactions'])

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
    INSERT INTO transactions (
        event_id, customer_id, account_id, transaction_type,
        amount, currency, merchant_id, country, device_id, event_timestamp
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_id) DO NOTHING;
"""

if __name__ == "__main__":
    print("Demarrage du consumer. Ctrl+C pour arreter.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Erreur Kafka : {msg.error()}")
                continue

            event = json.loads(msg.value())
            cursor.execute(INSERT_QUERY, (
                event["event_id"], event["customer_id"], event["account_id"],
                event["transaction_type"], event["amount"], event["currency"],
                event["merchant_id"], event["country"], event["device_id"],
                event["timestamp"]
            ))
            print(f"Transaction inseree : {event['event_id']} ({event['amount']} EUR)")

    except KeyboardInterrupt:
        print("\nArret demande.")
    finally:
        cursor.close()
        pg_conn.close()
        consumer.close()