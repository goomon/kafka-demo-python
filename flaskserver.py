import logging
import json
import os
import sys

from confluent_kafka import Producer
from dotenv import load_dotenv
from flask import Flask
import linecache

from callback.producer_callback import delivery_callback

app = Flask(__name__)

load_dotenv(verbose=True)
BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
RAW_SENSOR_DATA = os.getenv("RAW_SENSOR_DATA")
conf_producer = {"bootstrap.servers": BOOTSTRAP_SERVER}
producer = Producer(conf_producer)

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))

@app.route("/sensordata/<int:line_num>")
def generate_data(line_num):
    line = linecache.getline("data/accelerometer.csv", int(line_num))
    line = line.rstrip().split(",")
    mock_data = {}
    mock_data["time_stamp"] = float(line[0])
    mock_data["x"] = float(line[1])
    mock_data["y"] = float(line[2])
    mock_data["z"] = float(line[3])

    try:
        producer.produce(RAW_SENSOR_DATA, json.dumps(mock_data), callback=delivery_callback)
        producer.flush()
        return json.dumps({"success": True}), 200, {"ContentType": "application/json"}
    except RuntimeError:
        return json.dumps({"success": False}), 404, {"ContentType": "application/json"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3030)
