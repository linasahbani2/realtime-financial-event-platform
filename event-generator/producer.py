import json
import os
import time
import uuid
import random
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
producer = Producer({'bootstrap.servers': os.environ["KAFKA_BOOTSTRAP"]})

# Chaque client a un montant "habituel" (entre 20 et 150 EUR)
CUSTOMERS = {f"C{10000 + i}": random.uniform(20, 150) for i in range(10)}


def delivery_report(err, msg):
    if err is not None:
        print(f"Echec de livraison : {err}")


def normal_amount(customer_id):
    """Montant normal : proche du montant habituel du client (+/- 15 %)."""
    typical = CUSTOMERS[customer_id]
    return round(max(2, random.gauss(typical, typical * 0.15)), 2)


def make_event(customer_id, amount):
    return {
        "event_id": f"TX_{uuid.uuid4().hex[:8]}",
        "customer_id": customer_id,
        "account_id": f"ACC{customer_id[1:]}",
        "transaction_type": random.choice(["TRANSFER", "CARD_PAYMENT", "WITHDRAWAL", "DEPOSIT"]),
        "amount": amount,
        "currency": "EUR",
        "merchant_id": f"M{random.randint(100, 200)}",
        "country": "TN",
        "device_id": f"D{customer_id[1:]}",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def send(event):
    producer.produce(
        topic="financial.transactions",
        key=event["customer_id"],
        value=json.dumps(event),
        callback=delivery_report
    )
    producer.poll(0)


if __name__ == "__main__":
    print("Demarrage du simulateur. Ctrl+C pour arreter.")
    try:
        while True:
            customer_id = random.choice(list(CUSTOMERS))
            r = random.random()

            if r < 0.02:
                # Scenario A : montant tres superieur a l'habitude du client
                amount = round(CUSTOMERS[customer_id] * random.uniform(30, 100), 2)
                print(f"[ANOMALIE INJECTEE - montant] {customer_id} : {amount} EUR")
                send(make_event(customer_id, amount))

            elif r < 0.03:
                # Scenario B : rafale de 15 transactions en 3 secondes
                print(f"[ANOMALIE INJECTEE - rafale] {customer_id} : 15 transactions")
                for _ in range(15):
                    send(make_event(customer_id, normal_amount(customer_id)))
                    time.sleep(0.2)

            else:
                # Transaction normale
                send(make_event(customer_id, normal_amount(customer_id)))
                print(f"transaction normale : {customer_id}")

            time.sleep(1)
    except KeyboardInterrupt:
        print("\nArret demande.")
    finally:
        producer.flush()