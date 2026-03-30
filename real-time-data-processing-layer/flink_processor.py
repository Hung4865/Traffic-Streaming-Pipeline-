import json
import math
import os
from datetime import datetime, timezone

import psycopg2
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_METRICS_TOPIC = os.getenv(
    "KAFKA_METRICS_TOPIC",
    os.getenv("KAFKA_OUTPUT_TOPIC", "iot-camera-metrics")
)
FLINK_GROUP_ID = os.getenv("FLINK_GROUP_ID", "flink-processor-metrics-v1")
TIMESCALEDB_URL = os.getenv(
    "TIMESCALEDB_URL",
    "postgresql://postgres:supersecretpostgres@timescaledb:5432/traffic_metrics"
)


def is_valid_number(value) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def extract_total_vehicles(data: dict) -> int:
    value = data.get("total_vehicles", 0)
    if isinstance(value, int):
        return value

    detections = data.get("detections", [])
    if isinstance(detections, list):
        return len(detections)

    return 0


def extract_avg_speed(data: dict) -> float:
    value = data.get("avg_speed", 0)
    if is_valid_number(value):
        return float(value)

    detections = data.get("detections", [])
    if not isinstance(detections, list):
        return 0.0

    speeds = []
    for det in detections:
        if not isinstance(det, dict):
            continue

        speed = det.get("speed_kph")
        if is_valid_number(speed):
            speeds.append(float(speed))

    return sum(speeds) / len(speeds) if speeds else 0.0


def extract_event_time(data: dict) -> datetime:
    raw_timestamp = data.get("frame_timestamp")
    if raw_timestamp is None:
        return datetime.now(timezone.utc)

    try:
        ts = float(raw_timestamp)
        if ts > 1e11:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)


def process_and_sink(data_str: str) -> str:
    try:
        data = json.loads(data_str)

        total_vehicles = extract_total_vehicles(data)
        avg_speed = extract_avg_speed(data)
        event_time = extract_event_time(data)

        with psycopg2.connect(TIMESCALEDB_URL) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO traffic_metrics (time, total_vehicles, avg_speed_kmh)
                    VALUES (%s, %s, %s)
                    """,
                    (event_time, total_vehicles, avg_speed),
                )

        print(
            f"[Flink] inserted: time={event_time.isoformat()}, "
            f"total_vehicles={total_vehicles}, avg_speed_kmh={avg_speed}"
        )
        return "OK"

    except Exception as e:
        print(f"[Flink] processing error: {e}")
        return "ERROR"


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_props = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": FLINK_GROUP_ID,
        "auto.offset.reset": "earliest",
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics=KAFKA_METRICS_TOPIC,
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props,
    )
    kafka_consumer.set_start_from_earliest()

    stream = env.add_source(kafka_consumer)
    stream.map(process_and_sink, output_type=Types.STRING())

    env.execute("Traffic Data Pipeline")


if __name__ == "__main__":
    main()