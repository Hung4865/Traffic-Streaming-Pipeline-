import os

KAFKA_INPUT_TOPIC = os.environ.get('KAFKA_INPUT_TOPIC', 'iot-camera-frames')
KAFKA_OUTPUT_TOPIC = os.environ.get('KAFKA_OUTPUT_TOPIC', 'iot-camera-frames-processed')
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
KAFKA_GROUP_ID = os.environ.get('KAFKA_GROUP_ID', 'streaming-consumer-group')