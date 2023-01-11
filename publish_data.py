import logging
import requests
import sys
import time
from argparse import ArgumentParser

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler(sys.stdout))

if __name__ == "__main__":
    parser = ArgumentParser(prog="publish_data")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--delay", type=float, default=0.5)
    args = parser.parse_args()

    url_factory = lambda n: f"http://localhost:3030/sensordata/{n}"
    for i in range(1, args.count):
        try:
            requests.get(url_factory(i))
            time.sleep(args.delay)
        except Exception:
            logger.debug("Connection Error!")
