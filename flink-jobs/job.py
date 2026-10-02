import json
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

env = StreamExecutionEnvironment.get_execution_environment()
env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
env.add_jars("file:///home/lina/rt-fiep/flink-jobs/jars/flink-sql-connector-kafka-3.1.0-1.18.jar")

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
        # Deux memoires PAR CLIENT
        self.amounts = runtime_context.get_list_state(
            ListStateDescriptor("amounts", Types.DOUBLE())
        )
        self.timestamps = runtime_context.get_list_state(
            ListStateDescriptor("timestamps", Types.LONG())
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
                    "event_id": event["event_id"],
                    "customer_id": event["customer_id"],
                    "amount": amount,
                    "recent_average": round(average, 2),
                    "event_timestamp": event["timestamp"],
                })

        # Un montant anormal n'entre pas dans l'historique (sinon il fausse la moyenne)
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
                "event_id": event["event_id"],
                "customer_id": event["customer_id"],
                "amount": amount,
                "tx_count": len(recent),
                "event_timestamp": event["timestamp"],
            })


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