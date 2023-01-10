import argparse
import os
from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic

load_dotenv(verbose=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="create_topic")
    parser.add_argument("--np", type=int, default=1, help="number of partitions")
    parser.add_argument("--rf", type=int, default=1, help="replication factors")
    args = parser.parse_args()

    BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
    RAW_SENSOR_DATA = os.getenv("RAW_SENSOR_DATA")
    PREPROCESSED_DATA = os.getenv("PREPROCESSED_DATA")
    FEATURE_DATA = os.getenv("FEATURE_DATA")

    conf = {"bootstrap.servers": BOOTSTRAP_SERVER}
    admin = AdminClient(conf)

    topic_names = [RAW_SENSOR_DATA, PREPROCESSED_DATA, FEATURE_DATA]
    new_topics = [NewTopic(topic, num_partitions=args.np, replication_factor=args.rf) for topic in topic_names]
    fs = admin.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
