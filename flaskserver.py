import json
import linecache
from configparser import ConfigParser

from confluent_kafka import Producer
from flask import Flask
from mock_generator import MockSensorDataGenerator

from callback.producer_callback import delivery_callback

app = Flask(__name__)

# Constants
RAW_SENSOR_DATA = "raw_sensor_data"

# Producer settings
"""
config_parser = ConfigParser()
config_parser.read("config/v1.ini")
config = dict(config_parser["default"])
config.update(config_parser["producer"])
producer = Producer(config)
"""

DEBUG = "DEBUG"
ERROR = "ERROR"
import time
import pandas as pd

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



@app.route("/read_wesad_data")
def read_wesad_data():
    file_path = "data/wesad_chest_small_3classes.csv"
    st_load = time.time()
    print(f"[{DEBUG}] === Before loading data ===== ")
    df = pd.read_csv(file_path)
    print(f"[{DEBUG}] === Took {time.time() - st_load} seconds to load ===")

    n_rows = 4
    start_idx = 0
    end_idx = 0
    cols = ['chestAcc',  "chestECG",  "chestEMG",  "chestEDA",  "chestTemp", "chestResp",  "label", "user_id", "timestamp"]
    feat_cols =['chestAcc',  "chestECG",  "chestEMG",  "chestEDA",  "chestTemp", "chestResp"]

    while end_idx < len(df):
        # time.sleep(1)
        end_idx = start_idx + n_rows
        cur_df = df.iloc[start_idx:end_idx]

        one_second_data = {}
        for col in cols:
            one_second_data[col] = cur_df[col].tolist()

        try:
            formatted_one_sec_data = {}
            formatted_one_sec_data['user_id'] = one_second_data['user_id'][0]
            formatted_one_sec_data['timestamp'] = one_second_data['timestamp'][0]
            formatted_one_sec_data['label'] = one_second_data['label'][0]
            formatted_one_sec_data['value'] = {}
            for feat_col in feat_cols:
                formatted_one_sec_data['value'][feat_col] = {}
                formatted_one_sec_data['value'][feat_col]['hz'] = n_rows
                formatted_one_sec_data['value'][feat_col]['value'] = one_second_data[feat_col]

            print(f"Formatted data we will send: {formatted_one_sec_data}")
            # producer.produce(RAW_SENSOR_DATA, json.dumps(one_second_data), callback=delivery_callback)
            # producer.flush()
            # json.dumps({"success": True}), 200, {"ContentType": "application/json"}
            print(f"[{DEBUG}] Produced data with indices: {start_idx}:{end_idx-1}")
        except RuntimeError:
            # json.dumps({"success": False}), 404, {"ContentType": "application/json"}
            print(f"[{ERROR}] Failed to produce data with indices: {start_idx}:{end_idx-1}")

        # break
        start_idx = end_idx
        if start_idx + n_rows >= len(df):
            break


@app.route("/")
def generate_random_data():
    try:
        mock_generator = MockSensorDataGenerator
        simulation_data = mock_generator.generate_data(user_id="sample_user_id")
        producer.produce(RAW_SENSOR_DATA, json.dumps(simulation_data), callback=delivery_callback)
        producer.flush()
        return json.dumps({"success": True}), 200, {"ContentType": "application/json"}

    except RuntimeError:
        return json.dumps({"success": False}), 404, {"ContentType": "application/json"}




# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=3030)

if __name__ == "__main__":
    read_wesad_data()

