import json
import logging
import math
import traceback
from collections import Counter
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from environment import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_INPUT_TOPIC,
    KAFKA_GROUP_ID,
    KAFKA_OUTPUT_TOPIC,
)
from VehicleDetectionTracker.VehicleDetectionTracker import VehicleDetectionTracker

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_timestamp(data: dict, message) -> tuple[float, datetime]:
    """
    Parse frame timestamp from payload or Kafka metadata.
    Returns:
        (timestamp_float_seconds, timestamp_datetime_utc)
    """
    raw_timestamp = data.get("frame_timestamp")

    if raw_timestamp is not None:
        timestamp_float = float(raw_timestamp)
    else:
        # kafka-python message.timestamp is usually milliseconds
        timestamp_float = float(getattr(message, "timestamp", 0))

    if timestamp_float > 1e11:
        timestamp_float /= 1000.0

    timestamp_dt = datetime.fromtimestamp(timestamp_float, tz=timezone.utc)
    return timestamp_float, timestamp_dt


def is_valid_number(value) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def extract_metrics_from_tracker_result(processed_result: dict) -> dict:
    """
    Map current VehicleDetectionTracker output into compact metrics payload.
    Expected tracker output fields:
      - number_of_vehicles_detected
      - detected_vehicles[]
      - detected_vehicles[i].speed_info.kph
    """
    detections = processed_result.get("detected_vehicles", [])
    if not isinstance(detections, list):
        detections = []

    total_vehicles = processed_result.get("number_of_vehicles_detected", len(detections))
    if not isinstance(total_vehicles, int):
        total_vehicles = len(detections)

    vehicle_counter = Counter()
    compact_detections = []
    speed_values = []

    for det in detections:
        if not isinstance(det, dict):
            continue

        vehicle_type = str(det.get("vehicle_type", "unknown"))
        vehicle_counter[vehicle_type] += 1

        speed_info = det.get("speed_info") or {}
        speed_kph = speed_info.get("kph")
        if is_valid_number(speed_kph):
            speed_values.append(float(speed_kph))

        compact_detections.append({
            "vehicle_id": det.get("vehicle_id"),
            "vehicle_type": vehicle_type,
            "detection_confidence": det.get("detection_confidence"),
            "vehicle_coordinates": det.get("vehicle_coordinates"),
            "speed_kph": float(speed_kph) if is_valid_number(speed_kph) else None,
            "speed_reliability": speed_info.get("reliability"),
            "direction_label": speed_info.get("direction_label"),
            "direction": speed_info.get("direction"),
        })

    avg_speed = sum(speed_values) / len(speed_values) if speed_values else 0.0

    return {
        "total_vehicles": total_vehicles,
        "avg_speed": avg_speed,
        "vehicle_count_by_type": dict(vehicle_counter),
        "detections": compact_detections,
    }


def build_output_payload(input_data: dict, timestamp_float: float, tracker_result: dict) -> dict:
    metrics = extract_metrics_from_tracker_result(tracker_result)

    return {
        "mac_address": input_data.get("mac_address"),
        "camera_id": input_data.get("camera_id"),
        "frame_timestamp": str(input_data.get("frame_timestamp", timestamp_float)),
        "total_vehicles": metrics["total_vehicles"],
        "avg_speed": metrics["avg_speed"],
        "vehicle_count_by_type": metrics["vehicle_count_by_type"],
        "detections": metrics["detections"],
    }


def main():
    logging.info("Starting Standalone Frame Processor...")

    consumer = KafkaConsumer(
        KAFKA_INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        auto_offset_reset="earliest",
        max_poll_interval_ms=900000,
        max_poll_records=1,
        session_timeout_ms=120000,
        heartbeat_interval_ms=40000,
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    )

    tracker = VehicleDetectionTracker()
    logging.info("Ready to process frames.")

    try:
        for message in consumer:
            try:
                data = message.value

                # Handle double-encoded JSON
                if isinstance(data, str):
                    try:
                        data = json.loads(data)
                    except json.JSONDecodeError:
                        logging.warning("Skipping invalid JSON frame: %s", data)
                        continue

                if not isinstance(data, dict):
                    logging.warning("Skipping invalid payload type: %s", type(data))
                    continue

                frame_data = data.get("frame_data")
                if not frame_data:
                    logging.warning("Skipping frame without frame_data")
                    continue

                timestamp_float, timestamp_dt = parse_timestamp(data, message)

                processed_result = tracker.process_frame_base64(frame_data, timestamp_dt)

                if not isinstance(processed_result, dict):
                    logging.warning("Skipping frame because tracker result is not dict")
                    continue

                if processed_result.get("error"):
                    logging.warning("Tracker error: %s", processed_result.get("error"))
                    continue

                output_payload = build_output_payload(
                    input_data=data,
                    timestamp_float=timestamp_float,
                    tracker_result=processed_result,
                )

                # Important: send only compact metrics payload, not images/base64
                future = producer.send(KAFKA_OUTPUT_TOPIC, value=output_payload)
                future.get(timeout=30)

                logging.info(
                    "Processed frame from %s at %s | total_vehicles=%s | avg_speed=%.2f",
                    data.get("camera_id"),
                    timestamp_float,
                    output_payload["total_vehicles"],
                    output_payload["avg_speed"],
                )

            except Exception as e:
                logging.error("Error processing frame: %s", e)
                logging.error(traceback.format_exc())
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception:
            pass

        try:
            consumer.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()