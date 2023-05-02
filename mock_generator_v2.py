import time
from collections import defaultdict
from typing import List, Generator, Optional
from configparser import ConfigParser
import json
from confluent_kafka import Producer
from callback.producer_callback import delivery_callback
from schema.chest_schema import ChestDeviceSensorRecord, ChestAxis, \
    ChestDeviceSensorValue

SAMPLING_RATE = 700


RAW_SENSOR_DATA = "raw_sensor_data"

# Producer settings
# """
config_parser = ConfigParser()
config_parser.read("config/v1.ini")
config = dict(config_parser["default"])
config.update(config_parser["producer"])
producer = Producer(config)

def read_lines(filename: str) -> Generator[str, None, None]:
    if filename is None:
        filename = "./data/respiban.txt"
    while True:
        with open(filename) as f:
            for line in f:
                yield line.strip()


def calc_sample_rate() -> int:
    return int(2 * (1 - 0.5))


class MockSensorDataGeneratorV2:
    sampling_rate: int = calc_sample_rate()
    counter: dict[str, int] = defaultdict(lambda: 0)
    timestamp_map: dict[str, float] = defaultdict(time.time)
    file_reader: Generator[str, None, None] = read_lines(None)
    record_queue: List[ChestDeviceSensorValue] = []

    @staticmethod
    def generate_data(user_id: str) -> Optional[ChestDeviceSensorRecord]:
        MockSensorDataGeneratorV2.timestamp_map[user_id] += 1
        MockSensorDataGeneratorV2.counter[user_id] += 1
        # Data generating function is called every one second.
        timestamp = MockSensorDataGeneratorV2.timestamp_map[user_id]
        counter = MockSensorDataGeneratorV2.counter[user_id]

        chest_acc: List[ChestAxis] = []
        #chest_acc: List[int] = []
        chest_ecg: List[int] = []
        chest_eda: List[int] = []
        chest_emg: List[int] = []
        chest_temp: List[int] = []
        chest_resp: List[int] = []
        cnt = 0
        for line in MockSensorDataGeneratorV2.file_reader:
            line = line.split("\t")
            # Sampling rate of Respiban dataset is 700Hz.
            # if cnt > 700:
            if cnt > 699:
                break
            cnt += 1
            # Insert sensor data from target file.
            chest_ecg.append(int(line[0]))
            chest_eda.append(int(line[1]))
            chest_emg.append(int(line[2]))
            chest_temp.append(int(line[3]))
            #chest_acc.append(int(line[4]))
            chest_acc.append({
                "x": int(line[4]),
                "y": int(line[5]),
                "z": int(line[6])
            })
            chest_resp.append(int(line[7]))

        template: ChestDeviceSensorValue = {
            "chest_acc": {
                "hz": SAMPLING_RATE,
                "value": chest_acc
            },
            "chest_ecg": {
                "hz": SAMPLING_RATE,
                "value": chest_ecg
            },
            "chest_eda": {
                "hz": SAMPLING_RATE,
                "value": chest_eda
            },
            "chest_emg": {
                "hz": SAMPLING_RATE,
                "value": chest_emg
            },
            "chest_temp": {
                "hz": SAMPLING_RATE,
                "value": chest_temp
            },
            "chest_resp": {
                "hz": SAMPLING_RATE,
                "value": chest_resp
            }
        }
        MockSensorDataGeneratorV2.record_queue.append(template)
        # If record_queue size is greater than window size, pop it.
        if len(MockSensorDataGeneratorV2.record_queue) > 2:
            MockSensorDataGeneratorV2.record_queue = MockSensorDataGeneratorV2.record_queue[1:]

        if len(MockSensorDataGeneratorV2.record_queue) == 2 and counter % MockSensorDataGeneratorV2.sampling_rate == 0:
            return {
                "user_id": user_id,
                "timestamp": timestamp,
                "window_size": 2,
                "value": MockSensorDataGeneratorV2.record_queue,
                "label":0

            }
        else:
            return None


def do_simulation():
    try:
        mock_generator = MockSensorDataGeneratorV2
        simulation_data = mock_generator.generate_data(user_id="sample_user_id")
        producer.produce(RAW_SENSOR_DATA, json.dumps(simulation_data), callback=delivery_callback)
        producer.flush()
        return json.dumps({"success": True}), 200, {"ContentType": "application/json"}

    except RuntimeError:
        return json.dumps({"success": False}), 404, {"ContentType": "application/json"}

if __name__ == "__main__":
    do_simulation()

