import logging
import json
import sys
import time
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from copy import deepcopy

from confluent_kafka import Consumer, Producer
from datetime import datetime, timedelta

from callback.producer_callback import delivery_callback

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))

RAW_SENSOR_DATA = "raw_sensor_data"
PREPROCESSED_DATA = "preprocessed_data"
FEATURE_DATA = "feature_data"

def preprocess_data(msg):
    json_loads = json.loads(msg)
    mock_data = {}

    # data validation and create json data dict
    mock_data["time_stamp"] = int(json_loads["time_stamp"]) if json_loads.get("time_stamp") else None
    mock_data["x"] = float(json_loads["x"]) if json_loads.get("x") else None
    mock_data["y"] = float(json_loads["y"]) if json_loads.get("y") else None
    mock_data["z"] = float(json_loads["z"]) if json_loads.get("z") else None
    return mock_data

def feature_extraction(msg_buf, mode, start_time):
    # 어디까지 데이터 읽을 것인지 마지막 시점 설정해야 함 (lastTime가지고 설정)
    end_time = start_time + timedelta(seconds=window_size)
    feature_data = {}
    avg_x = 0.0
    avg_y = 0.0
    avg_z = 0.0

    if mode == 1:
        for i in range(len(msg_buf)):
            avg_x += msg_buf[i]["x"]
            avg_y += msg_buf[i]["y"]
            avg_z += msg_buf[i]["z"]

        feature_data["time_stamp"] = float(time.time())
        feature_data["x"] = avg_x / len(msg_buf)
        feature_data["y"] = avg_y / len(msg_buf)
        feature_data["z"] = avg_z / len(msg_buf)
        producer2.produce(FEATURE_DATA, json.dumps(feature_data), callback=delivery_callback)
        producer2.flush()
        return True
    else:
        for i in range(len(msg_buf)):
            temp_time = datetime.fromtimestamp(time.time())
            if temp_time >= start_time and temp_time <= end_time:
                avg_x += msg_buf[i]["x"]
                avg_y += msg_buf[i]["y"]
                avg_z += msg_buf[i]["z"]

        feature_data["time_stamp"] = str(time.time())
        feature_data["x"] = avg_x / len(msg_buf)
        feature_data["y"] = avg_y / len(msg_buf)
        feature_data["z"] = avg_z / len(msg_buf)

        producer2.produce(FEATURE_DATA, json.dumps(feature_data))
        producer2.flush()
        return True

if __name__ == "__main__":
    parser = ArgumentParser(prog="consume_data")
    parser.add_argument("--config", type=FileType("r"), default="config/v1.ini", help="config file path")
    args = parser.parse_args()

    # Configuration loading
    config_parser = ConfigParser()
    config_parser.read_file(args.config)
    default_config = dict(config_parser["default"])

    # Consumer settings
    consumer_config = deepcopy(default_config)
    consumer_config.update(config_parser["consumer"])
    consumer = Consumer(consumer_config)

    # Producer settings
    producer_config = deepcopy(default_config)
    producer_config.update(config_parser["producer"])
    producer1 = Producer(producer_config)
    producer2 = Producer(producer_config)

    # seconds
    window_size = 0.1
    overlapRatio = 0.5

    sampling_cycle = window_size * (1 - overlapRatio)
    is_first = True
    is_feature_engineering = True
    last_updated = datetime.now()
    msg_buf = []
    consumer.subscribe([RAW_SENSOR_DATA])
    try:
        while True:
            msg = consumer.poll(timeout=3)
            if msg is None:
                logger.debug("Waiting...")
            elif msg.error():
                logger.debug(f"ERROR: {msg.error()}")
            else:
                logger.debug(f"Consumes event from topic {msg.topic()}")
                current_time = datetime.now()
                data = preprocess_data(msg.value())
                msg_buf.append(data)
                producer1.produce(PREPROCESSED_DATA, json.dumps(data), callback=delivery_callback)
                producer1.flush()

                if current_time - timedelta(seconds=window_size) >= last_updated:
                    if is_feature_engineering:
                        is_feature_engineering = False
                        if is_first:
                            is_first = False
                            # Time window: startTime ~ startTime+windowSize
                            # Feature engineering: msgBuff 데이터 사용(누적된 데이터셋)
                            if feature_extraction(msg_buf, 1, last_updated):
                                # 다음 Time window 시작점 셋업: lastUpdated = lastUpdated + samplingCycle
                                last_updated = last_updated + timedelta(seconds=sampling_cycle)
                                is_feature_engineering = True
                        else:
                            # Time window: lastUpdated ~ lastUpdated + windowSize
                            # Feature engineering: Buff 데이터 사용(누적된 데이터셋)
                            if feature_extraction(msg_buf, 2, last_updated):
                                # 다음 Time window 시작점 셋업: lastUpdated = lastUpdated + samplingCycle
                                last_updated = last_updated + timedelta(seconds=sampling_cycle)
                                is_feature_engineering = True
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
