from argparse import ArgumentParser, FileType
from configparser import ConfigParser

from dotenv import load_dotenv
from confluent_kafka.admin import AdminClient, NewTopic

load_dotenv(verbose=True)

if __name__ == "__main__":
    parser = ArgumentParser(prog="create_topic")
    parser.add_argument("--config", type=FileType("r"), default="config/v1.ini", help="default config file path")
    parser.add_argument("--topic_config", type=FileType("r"), default="config/topic/v1.ini", help="topic config file path")
    args = parser.parse_args()

    config_parser = ConfigParser()
    config_parser.read_file(args.config)
    admin = AdminClient(dict(config_parser["default"]))

    config_parser.clear()
    config_parser.read_file(args.topic_config)
    new_topics = []
    for k in config_parser.__dict__["_sections"].keys():
        np = config_parser.getint(k, "num.partitions", fallback=1)
        rf = config_parser.getint(k, "replication.factor", fallback=1)
        new_topics.append(NewTopic(k, num_partitions=np, replication_factor=rf))

    fs = admin.create_topics(new_topics)
    for topic, f in fs.items():
        try:
            f.result()
            print(f"Topic {topic} created")
        except Exception as e:
            print(f"Failed to create topic {topic}: {e}")
