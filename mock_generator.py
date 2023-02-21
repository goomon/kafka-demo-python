import random
import time
from collections import defaultdict

from config.constants import *
from schema.schema import SensorRecord


class MockSensorDataGenerator:
    timestamp_map: dict[str, float] = defaultdict(time.time)

    @staticmethod
    def generate_data(user_id: str) -> SensorRecord:
        MockSensorDataGenerator.timestamp_map[user_id] += 1
        timestamp = MockSensorDataGenerator.timestamp_map[user_id] + 1
        template: SensorRecord = {
            "user_id": user_id,
            "timestamp": int(timestamp * 1000),
            "value": {
                "acc": {
                    "hz": ACC_HZ,
                    "value": [{
                        "x": random.uniform(-20, 20),
                        "y": random.uniform(-20, 20),
                        "z": random.uniform(-20, 20)
                    } for _ in range(ACC_HZ)]
                },
                "bvp": {
                    "hz": BVP_HZ,
                    "value": [random.uniform(-400, 1200) for _ in range(BVP_HZ)]
                },
                "eda": {
                    "hz": EDA_HZ,
                    "value": [random.uniform(0.3, 1.5) for _ in range(EDA_HZ)]
                },
                "hr": {
                    "hz": HR_HZ,
                    "value": [random.uniform(80, 10) for _ in range(HR_HZ)]
                },
                "temp": {
                    "hz": TEMP_HZ,
                    "value": [random.gauss(36.5, 1) for _ in range(TEMP_HZ)]
                }
            }
        }
        return template
