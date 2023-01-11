import logging
import json
import sys
from configparser import ConfigParser

from confluent_kafka import Producer
from flask import Flask
import linecache

from callback.producer_callback import delivery_callback

app = Flask(__name__)

# Constants
RAW_SENSOR_DATA = "raw_sensor_data"

# Logging options
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))

# Producer settings
config_parser = ConfigParser()
config_parser.read("config/v1.ini")
config = dict(config_parser["default"])
config.update(config_parser["producer"])
producer = Producer(config)

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
