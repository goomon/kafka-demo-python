from logger.TestLogger import TestLogger


def delivery_callback(err, msg):
    logger = TestLogger()
    if err:
        logger.debug(f"ERROR: Message failed delivery: {err}")
        raise RuntimeError()
    else:
        topic = msg.topic()
        key = msg.key().decode("utf-8") if msg.key() else "null"
        value = msg.value().decode("utf-8")
        logger.debug(f"Produced event to topic {topic}: key = {key:12} value = {value:12}")
