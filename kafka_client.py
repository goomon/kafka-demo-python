import time
from configparser import ConfigParser

from confluent_kafka import Producer
from locust import events


class KafkaClient:

    def __init__(self, **kwargs):
        config_path = kwargs.get("config_path")
        if config_path is None:
            print("here")
            config_path = "config/v1.ini"
        config_parser = ConfigParser()
        config_parser.read(config_path)
        config = dict(config_parser["default"])
        config.update(config_parser["producer"])
        self.producer = Producer(config)

    @staticmethod
    def delivery_callback(err, msg):
        topic = msg.topic()
        start_time = msg.timestamp()[1]
        end_time = int(time.time() * 1000)
        elapsed_time = end_time - start_time
        if err:
            request_data = dict(request_type="ENQUEUE",
                                name=topic,
                                response_time=elapsed_time,
                                exception=str(err),
                                context=None,
                                reponse_length=0)
            events.request.fire(**request_data)
        else:
            request_data = dict(request_type="ENQUEUE",
                                name=topic,
                                response_time=elapsed_time,
                                exception=None,
                                context=None,
                                response_length=len(msg))
            events.request.fire(**request_data)

    def send(self, topic, key=None, message=None):
        self.producer.produce(topic=topic, key=key, value=message, callback=KafkaClient.delivery_callback)
        self.producer.flush()
