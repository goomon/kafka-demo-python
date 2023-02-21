import json
import time
import uuid

from locust import task, User

from config.constants import MAIN_TOPIC
from kafka_client import KafkaClient
from mock_generator import MockSensorDataGenerator


class KafkaLocust(User):
    mock_generator = MockSensorDataGenerator
    client = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.user_id = None
        KafkaLocust.client = KafkaClient()

    def on_start(self):
        self.user_id = str(uuid.uuid4())
        return super().on_start()

    @task
    def send_data(self):
        msg = KafkaLocust.mock_generator.generate_data(self.user_id)
        self.client.send(MAIN_TOPIC, key=self.user_id, message=json.dumps(msg))
        time.sleep(0.5)
