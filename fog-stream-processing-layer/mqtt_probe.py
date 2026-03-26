import os
import ast
import base64
from pathlib import Path
import paho.mqtt.client as mqtt

OUTPUT_DIR = Path("probe_output")
OUTPUT_DIR.mkdir(exist_ok=True)

BROKER = "localhost"
PORT = 1883
TOPIC = "iot-camera-frames"

count = 0
MAX_MSG = 3

def on_connect(client, userdata, flags, rc):
    print("Connected with result code:", rc)
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    global count
    payload = msg.payload.decode("utf-8", errors="ignore")

    data = ast.literal_eval(payload)
    frame_b64 = data["frame_data"]
    ts = data["frame_timestamp"]
    cam = data["camera_id"]

    img_bytes = base64.b64decode(frame_b64)
    out_file = OUTPUT_DIR / f"{cam}_{ts}.jpg"
    with open(out_file, "wb") as f:
        f.write(img_bytes)

    count += 1
    print(f"Saved: {out_file}")

    if count >= MAX_MSG:
        client.disconnect()

client = mqtt.Client()
client.username_pw_set("mosquitto", "mosquitto")
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT, 60)
client.loop_forever()