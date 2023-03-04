import json
import random
import time
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from copy import deepcopy
from datetime import datetime, timedelta

from confluent_kafka import Consumer, Producer

from callback.producer_callback import delivery_callback
from logger.TestLogger import TestLogger

from itertools import islice

import pandas as pd
import numpy as np

RAW_SENSOR_DATA = "raw_sensor_data"
PREPROCESSED_DATA = "preprocessed_data"
FEATURE_DATA = "feature_data"

WINDOW_SIZE = 2
OVERLAP_RATIO = 0.5
SR = 1 # Sampling Rate

logger = TestLogger()

MODALITIES = ["acc", "bvp", "eda", "hr"]

def preprocess_data(msg):
    # TODO: Load json and transform it to dict
    #json_loads = json.loads(msg)
    # tentative mockup data just for testing
    dict = {
      "user_id": 1,
      "timestamp": 12312412312,
      "value": {
        "acc": {
          "hz": 32,
          "value": [
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
            {"x": 0.1, "y": 0.1, "z": 0.1},
          ]
        },
        "bvp": {
          "hz": 64,
          "value": [
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
            0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,
          ]
        },
        "eda": {
          "hz": 4,
          "value": [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1,]
        },
        "hr": {
          "hz": 1,
          "value": [0.1, 0.1]
        }
      }
    }

    # (1) Flat columns
    #int(json_loads["time_stamp"]) if json_loads.get("time_stamp") else None # data validation and create json data dict
    signal = {}#pd.DataFrame(columns=["acc_x", "acc_y", "acc_z", "bvp", "eda", "hr"])
    acc_x = []
    acc_y = []
    acc_z = []
    for sample in dict["value"]["acc"]["value"]:
        acc_x.append(sample["x"])
        acc_y.append(sample["y"])
        acc_z.append(sample["z"])
    signal["acc_x"] = np.array(acc_x)
    signal["acc_y"] = np.array(acc_y)
    signal["acc_z"] = np.array(acc_z)
    signal["bvp"] = np.array(dict["value"]["bvp"]["value"])
    signal["eda"] = np.array(dict["value"]["eda"]["value"])
    signal["hr"] = np.array(dict["value"]["hr"]["value"])

    print("COMPLETE: (1) Flat columns")

    #data_all["x"] = float(json_loads["x"]) if json_loads.get("x") else None
    #data_all["y"] = float(json_loads["y"]) if json_loads.get("y") else None
    #data_all["z"] = float(json_loads["z"]) if json_loads.get("z") else None

    # (2) Downsampling
    # calculate the lowest sampling rate
    lowest_sr = 256
    for modal in MODALITIES:
        modal_sr = dict["value"][modal]["hz"]
        lowest_sr = modal_sr if modal_sr < lowest_sr else lowest_sr
    def downsampling(rows, proportion=1):
        return list(islice(rows, 0, len(rows), int(1/proportion)))
    signal["acc_x"] = downsampling(rows=signal["acc_x"], proportion=lowest_sr/dict["value"]["acc"]["hz"])
    signal["acc_y"] = downsampling(rows=signal["acc_y"], proportion=lowest_sr / dict["value"]["acc"]["hz"])
    signal["acc_z"] = downsampling(rows=signal["acc_z"], proportion=lowest_sr / dict["value"]["acc"]["hz"])
    signal["bvp"] = downsampling(rows=signal["bvp"], proportion=lowest_sr / dict["value"]["bvp"]["hz"])
    signal["eda"] = downsampling(rows=signal["eda"], proportion=lowest_sr / dict["value"]["eda"]["hz"])
    signal["hr"] = downsampling(rows=signal["hr"], proportion=lowest_sr / dict["value"]["hr"]["hz"])

    print("COMPLETE: (2) Downsampling")

    # (3) Generate timestamp & Merge arrays to DataFrame
    df = pd.DataFrame(columns=["ts", "acc_x", "acc_y", "acc_z", "bvp", "eda", "hr"])
    start_ts = dict["timestamp"]
    for i in range(len(signal["acc_x"])):
        curr_ts = datetime.fromtimestamp(start_ts) + timedelta(seconds=(i*(1/lowest_sr)))
        df = df.append({"ts": int(time.mktime(curr_ts.timetuple())),
                   "acc_x":signal["acc_x"][i],
                   "acc_y": signal["acc_y"][i],
                   "acc_z": signal["acc_z"][i],
                   "bvp": signal["bvp"][i],
                   "eda": signal["eda"][i],
                   "hr": signal["hr"][i],
                   },
                  ignore_index=True)

    print("COMPLETE: (3) Generate timestamp & Merge arrays to DataFrame")

    # (4) Drop samples with missing data
    df = df.dropna(axis=0)
    df.set_index(keys="ts", inplace=True, drop=True)
    df.index = df.index.astype(int)

    return df, lowest_sr


def feature_extraction(msg_buf, mode, start_time):  # (DataFrame, ?, datetime)
    # 어디까지 데이터 읽을 것인지 마지막 시점 설정해야 함 (lastTime가지고 설정)

    # (1) Calculate start and end timestamps
    # end_time = start_time + timedelta(seconds=WINDOW_SIZE)
    start_ts = time.mktime(start_time.timetuple())
    end_ts = time.mktime((start_time + timedelta(seconds=WINDOW_SIZE)).timetuple())

    WINDOW_LEN = int(WINDOW_SIZE / SR)
    # (2) Extract features
    FEATURES = ["acc_x_mean", "acc_x_std", "acc_y_mean", "acc_y_std", "acc_z_mean", "acc_z_std",
                "bvp_mean", "bvp_std",
                "eda_mean", "eda_std", "eda_min", "eda_max"
                                                  "hr_mean", "hr_std", "hr_min", "hr_max"]
    sample_df = msg_buf[(msg_buf.index >= int(start_ts)) & (msg_buf.index <= int(end_ts))]
    # TODO: For now, I assume that there is a matched ts in df with start_time.
    sample_feat = {}

    # Accelerometer features
    acc_x_vals = sample_df["acc_x"].values[:]
    acc_y_vals = sample_df["acc_y"].values[:]
    acc_z_vals = sample_df["acc_z"].values[:]
    sample_feat["acc_x_mean"] = np.mean(acc_x_vals)
    sample_feat["acc_x_std"] = np.std(acc_x_vals)
    sample_feat["acc_y_mean"] = np.mean(acc_y_vals)
    sample_feat["acc_y_std"] = np.std(acc_y_vals)
    sample_feat["acc_z_mean"] = np.mean(acc_z_vals)
    sample_feat["acc_z_std"] = np.std(acc_z_vals)

    # BVP features
    bvp_vals = sample_df["bvp"].values[:]
    sample_feat["bvp_mean"] = np.mean(bvp_vals)
    sample_feat["bvp_std"] = np.std(bvp_vals)

    # EDA features
    eda_vals = sample_df["eda"].values[:]
    sample_feat["eda_mean"] = np.mean(eda_vals)
    sample_feat["eda_std"] = np.std(eda_vals)
    sample_feat["eda_min"] = np.min(eda_vals)
    sample_feat["eda_max"] = np.max(eda_vals)

    # Heart Rate
    hr_vals = sample_df["hr"].values[:]
    sample_feat["hr_mean"] = np.mean(hr_vals)
    sample_feat["hr_std"] = np.std(hr_vals)
    sample_feat["hr_min"] = np.min(hr_vals)
    sample_feat["hr_max"] = np.max(hr_vals)

    sample_feat["ts"] = start_time

    # (3) dict to DataFrame
    df_feat = pd.DataFrame(columns=["ts",
                                        "acc_x_mean", "acc_x_std", "acc_y_mean", "acc_y_std", "acc_z_mean", "acc_z_std",
                                        "bvp_mean", "bvp_std",
                                        "eda_mean", "eda_std", "eda_min", "eda_max"
                                      "hr_mean", "hr_std", "hr_min", "hr_max"])
    df_feat = df_feat.append(sample_feat, ignore_index = True)

    if mode == 1:
        '''
        for i in range(len(msg_buf)):
        avg_x += msg_buf[i]["x"]
        avg_y += msg_buf[i]["y"]
        avg_z += msg_buf[i]["z"]

        feature_data["time_stamp"] = float(time.time())
        feature_data["x"] = avg_x / len(msg_buf)
        feature_data["y"] = avg_y / len(msg_buf)
        feature_data["z"] = avg_z / len(msg_buf)
        '''
        producer2.produce(FEATURE_DATA, json.dumps(df_feat), callback=delivery_callback)
        producer2.flush()
        return True
    else:
        '''
        for i in range(len(msg_buf)):
            temp_time = datetime.fromtimestamp(time.time())
            if temp_time >= start_time and temp_time <= end_time:
                avg_x += msg_buf[i]["x"]
                avg_y += msg_buf[i]["y"]
                avg_z += msg_buf[i]["z"]
        '''
        producer2.produce(FEATURE_DATA, json.dumps(df_feat))
        producer2.flush()
        return True


def extract_phenotype_from_features_and_labels(user_feature_df=None):
    # later we will pick up user_df from producer
    # now, generate assuming the structure we have

    user_feature_df = generate_sample_feature_data(num_records=100)
    # print(user_feature_df.dtypes)
    correlation = compute_pairwise_correlation(user_feature_df)


def generate_sample_feature_data(num_records):
    # Define the data types for each column
    feature_cols = ["acc_x_mean", "acc_x_std", "acc_y_mean", "acc_y_std",
                "acc_z_mean", "acc_z_std", "bvp_mean", "bvp_std",
                "eda_mean", "eda_std", "eda_min", "eda_max",
                "hr_mean", "hr_std", "hr_min", "hr_max"
                ]
    all_cols = feature_cols.copy()
    all_cols.append("label")
    user_feature_df = pd.DataFrame(columns=all_cols)
    generated_feat_label_list = generate_feature_value_pairs(len(all_cols) - 1,
                                                             num_records)
    for i in range(num_records):
        user_feature_df.loc[len(user_feature_df)] = generated_feat_label_list[i]
    user_feature_df['label'] = user_feature_df['label'].astype(int)
    return user_feature_df

def generate_feature_value_pairs(len_features, num_rows):
    feature_label_pairs = []
    for i in range(num_rows):
        feature_vals = [random.uniform(-1, 1) for i in range(len_features)]
        random_label = random.randint(0, 3)  # inclusive
        # print(f"len_features: {len(feature_vals)}, random_label: {random_label}")
        feature_vals.append(random_label)
        feature_label_pairs.append(feature_vals)
    return feature_label_pairs

def compute_pairwise_correlation(user_feature_df, method='spearman'):
    assert method in ['pearson', 'kendall', 'spearman']
    correlations = user_feature_df.corr(method=method)['label']
    print("correlatations: \n", correlations)

# if __name__ == "__main__":
#     extract_phenotype_from_features_and_labels()

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

    sampling_cycle = WINDOW_SIZE * (1 - OVERLAP_RATIO)
    is_first = True
    is_feature_engineering = True
    last_updated = datetime.now()
    #msg_buf = []
    msg_buf = pd.DataFrame(columns=["ts", "acc_x", "acc_y", "acc_z", "bvp", "eda", "hr"])
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
                data, SR = preprocess_data(msg.value()) # pd.DataFrame, samplingrate
                msg_buf = msg_buf.append(data, ignore_index=True)
                producer1.produce(PREPROCESSED_DATA, json.dumps(data), callback=delivery_callback)
                producer1.flush()

                if current_time - timedelta(seconds=WINDOW_SIZE) >= last_updated:
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


#empt_msg= []
#preprocess_data(empt_msg)