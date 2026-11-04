import json
import os
from datetime import datetime
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.kafka import (
    KafkaSource, KafkaOffsetsInitializer,
    KafkaSink, KafkaRecordSerializationSchema,
)
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ListStateDescriptor

# --- Parametres des regles ---
AMOUNT_FACTOR = 3            # regle 1 : alerte si montant > 3 x moyenne
HISTORY_SIZE = 5             # regle 1 : on garde les 5 derniers montants
MIN_HISTORY = 3              # regle 1 : il faut au moins 3 montants en memoire
VELOCITY_WINDOW_MS = 10_000  # regle 2 : fenetre de 10 secondes
VELOCITY_THRESHOLD = 8       # regle 2 : alerte a la 8e transaction dans la fenetre
GEO_WINDOW_MS = 60_000       # regle 3 : fenetre de 60 secondes
STRUCT_THRESHOLD = 10_000                # regle 4 : seuil de declaration (EUR)
STRUCT_LOWER = 0.9 * STRUCT_THRESHOLD    # regle 4 : "juste en dessous" = entre 9 000 et 10 000
STRUCT_WINDOW_MS = 600_000               # regle 4 : fenetre de 10 minutes
STRUCT_MIN_COUNT = 3                     # regle 4 : alerte a la 3e transaction suspecte

JARS_DIR = os.path.dirname(os.path.abspath(__file__))


# --- Niveaux de risque ---
def amount_risk(ratio):
    """ratio = montant / moyenne recente du client."""
    if ratio >= 30:
        return "CRITICAL"
    if ratio >= 10:
        return "HIGH"
    return "MEDIUM"


def geo_risk(gap_seconds):
    """Plus l'ecart entre les deux pays est court, plus c'est suspect."""
    return "CRITICAL" if gap_seconds < 15 else "HIGH"


env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.add_jars(f"file://{JARS_DIR}/jars/flink-sql-connector-kafka-3.1.0-1.18.jar")

# --- Source : lit les transactions ---
source = KafkaSource.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_topics("financial.transactions") \
    .set_group_id("flink-anomaly-detector") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")


class AnomalyDetector(KeyedProcessFunction):
    def open(self, runtime_context):
        # Quatre memoires PAR CLIENT
        self.amounts = runtime_context.get_list_state(
            ListStateDescriptor("amounts", Types.DOUBLE())
        )
        self.timestamps = runtime_context.get_list_state(
            ListStateDescriptor("timestamps", Types.LONG())
        )
        self.last_location = runtime_context.get_list_state(
            ListStateDescriptor("last_location", Types.STRING())
        )
        self.near_threshold = runtime_context.get_list_state(
            ListStateDescriptor("near_threshold", Types.LONG())
        )

    def process_element(self, value, ctx):
        event = json.loads(value)
        amount = float(event["amount"])
        ts_ms = int(datetime.fromisoformat(event["timestamp"]).timestamp() * 1000)

        # ---------- Regle 1 : montant anormal ----------
        past_amounts = list(self.amounts.get())
        is_amount_anomaly = False

        if len(past_amounts) >= MIN_HISTORY:
            average = sum(past_amounts) / len(past_amounts)
            if amount > average * AMOUNT_FACTOR:
                is_amount_anomaly = True
                yield json.dumps({
                    "alert_type": "AMOUNT_ANOMALY",
                    "risk_level": amount_risk(amount / average),
                    "event_id": event["event_id"],
                    "customer_id": event["customer_id"],
                    "amount": amount,
                    "recent_average": round(average, 2),
                    "event_timestamp": event["timestamp"],
                })

        if not is_amount_anomaly:
            past_amounts.append(amount)
            self.amounts.update(past_amounts[-HISTORY_SIZE:])

        # ---------- Regle 2 : trop de transactions en peu de temps ----------
        recent = [t for t in self.timestamps.get() if ts_ms - t <= VELOCITY_WINDOW_MS]
        recent.append(ts_ms)
        self.timestamps.update(recent)

        if len(recent) == VELOCITY_THRESHOLD:
            yield json.dumps({
                "alert_type": "VELOCITY_ANOMALY",
                "risk_level": "HIGH",
                "event_id": event["event_id"],
                "customer_id": event["customer_id"],
                "amount": amount,
                "tx_count": len(recent),
                "event_timestamp": event["timestamp"],
            })

        # ---------- Regle 3 : pays different trop rapidement ----------
        country = event["country"]
        stored = list(self.last_location.get())

        if stored:
            prev_country, prev_ts_str = stored[0].split("|")
            prev_ts = int(prev_ts_str)
            if country != prev_country and (ts_ms - prev_ts) < GEO_WINDOW_MS:
                gap_seconds = (ts_ms - prev_ts) // 1000
                yield json.dumps({
                    "alert_type": "GEOGRAPHIC_ANOMALY",
                    "risk_level": geo_risk(gap_seconds),
                    "event_id": event["event_id"],
                    "customer_id": event["customer_id"],
                    "amount": amount,
                    "from_country": prev_country,
                    "to_country": country,
                    "gap_seconds": gap_seconds,
                    "event_timestamp": event["timestamp"],
                })

        self.last_location.update([f"{country}|{ts_ms}"])

        # ---------- Regle 4 : structuring (montants juste sous le seuil) ----------
        if STRUCT_LOWER <= amount < STRUCT_THRESHOLD:
            near = [t for t in self.near_threshold.get() if ts_ms - t <= STRUCT_WINDOW_MS]
            near.append(ts_ms)
            self.near_threshold.update(near)

            if len(near) == STRUCT_MIN_COUNT:
                yield json.dumps({
                    "alert_type": "STRUCTURING",
                    "risk_level": "HIGH",
                    "event_id": event["event_id"],
                    "customer_id": event["customer_id"],
                    "amount": amount,
                    "tx_count": len(near),
                    "event_timestamp": event["timestamp"],
                })
                # On repart de zero : un nouvel episode demandera 3 nouvelles transactions
                self.near_threshold.clear()


alerts = stream \
    .key_by(lambda raw: json.loads(raw)["customer_id"], key_type=Types.STRING()) \
    .process(AnomalyDetector(), output_type=Types.STRING())

# --- Sink : ecrit les alertes dans Kafka ---
sink = KafkaSink.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_record_serializer(
        KafkaRecordSerializationSchema.builder()
            .set_topic("financial.alerts")
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
    ) \
    .build()

alerts.sink_to(sink)
alerts.print()

print("Job demarre, en attente de messages Kafka...")
env.execute("Anomaly Detection Job")