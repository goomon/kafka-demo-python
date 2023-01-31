import csv
import itertools
import json
import time
from concurrent import futures
from configparser import ConfigParser
from typing import Dict, Any, List

from confluent_kafka import Producer

from callback.producer_callback import delivery_callback
from logger.TestLogger import TestLogger
from schema.models import Accelerometer, HeartRate, SensorData, ElectrodermalActivity

# Logger
logger = TestLogger()

# Producer settings
config_parser = ConfigParser()
config_parser.read("config/v1.ini")
config = dict(config_parser["default"])
config.update(config_parser["producer"])
producer = Producer(config)

# This is dictionary for class constructor
clazz = {
    "ACC": Accelerometer,
    "HR": HeartRate,
    "EDA": ElectrodermalActivity
}

topics = {
    "ACC": "acc",
    "HR": "hr",
    "EDA": "eda"
}


def get_param(user_id: int, data_type: str, limit=10):
    """Returns parameter option for producer
    Args:
        user_id (int): target user id
        data_type (str): target data_type
        limit (int): time interval(sec) for data
    Returns:
        Dict[str, Any]: dictionary of parameter option
    Example:
        if data sampling rate is 32Hz and limit is 10, 320 rows of data will be published to Kafka brokers
   """
    return {
        "user_id": user_id,
        "data_type": data_type,
        "limit": limit
    }


def produce(param: Dict[str, Any]):
    """Produce sensor data in accordance of param option.
    Args:
        param (Dict[str, Any]): parameter option from `get_param` function.
    Returns:
        None
   """
    user_id = param.get("user_id")
    data_type = param.get("data_type")
    limit = param.get("limit")

    with open(f"data/WESAD/S{user_id}/{data_type}.csv") as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        # The first row is the initial time of the session expressed as unix timestamp in UTC.
        timestamp = float(next(reader)[0])
        # The second row is the sample rate expressed in Hz.
        hertz = float(next(reader)[0])

        rate = 1 / hertz
        total = int(limit * hertz)
        cnt = 0
        err_cnt = 0
        for row in reader:
            timestamp += rate
            wrapper = clazz[data_type]
            data: SensorData = wrapper(user_id, timestamp, *row)
            data: Dict[str, Any] = vars(data)

            # Publish sensor data
            try:
                producer.produce(topic=topics.get(data_type), value=json.dumps(data), key=str(user_id),
                                 callback=delivery_callback)
                producer.flush()
                cnt += 1
                time.sleep(rate)
            except RuntimeError as e:
                err_cnt += 1
                logger.warning(e)

            if cnt > total:
                break

    logger.info(f"user_id: {user_id} - data_type: {data_type} - {total - err_cnt}/{total} successes")


if __name__ == "__main__":
    users = [2, 3, 4]
    data_types = list(topics.keys())
    limits = [5]
    samples: List[Dict[str, Any]] = [get_param(*x) for x in itertools.product(users, data_types, limits)]

    with futures.ProcessPoolExecutor() as executor:
        executor.map(produce, samples)
