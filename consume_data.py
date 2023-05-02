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
from scipy.signal import resample
import joblib
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score


RAW_SENSOR_DATA = "raw_sensor_data"
PREPROCESSED_DATA = "preprocessed_data"
FEATURE_DATA = "feature_data"

WINDOW_SIZE = 2
OVERLAP_RATIO = 0.5
SR = 1 # Sampling Rate

logger = TestLogger()

SENSOR_COLUMNS = ['chestAcc', "chestECG", "chestEMG", "chestEDA", "chestTemp", "chestResp"]




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

def manage_raw_data_buffer(msg_value, org_raw_msg_df):
    print(f"----- in manage_raw_data_buffer() ---- org_raw_msg_df: {len(org_raw_msg_df)}-----")

    can_extract_features = False
    raw_msg_df = org_raw_msg_df
    data_to_process = None
    STATIC_TGT_SAMPLING_RATE = 4  # fixed; target
    arrived_timestamp = int(time.time()) * 1000

    template_row = {'label': msg_value['label'],
                    'timestamp': msg_value['timestamp'],
                    'user_id': msg_value['user_id'],
                    'arrived_timestamp': arrived_timestamp}

    fresh_df = pd.DataFrame(
        columns=['chestAcc', "chestECG", "chestEMG", "chestEDA", "chestTemp", "chestResp", "label", "user_id",
                 "timestamp", "arrived_timestamp"])

    feat_value_dict = {}
    for feat_col in SENSOR_COLUMNS:
        for i in range(WINDOW_SIZE):
            values = values.extend(msg_value['value'][i][feat_col]['value'])
        sampling_rate = msg_value['value'][feat_col]['hz']
        # print(f"====== sampling_rate: {sampling_rate}, feat_col: {feat_col}, values: {values}")
        if sampling_rate > STATIC_TGT_SAMPLING_RATE:
            source_signal = np.array(values)
            downsample_ratio = sampling_rate // STATIC_TGT_SAMPLING_RATE
            downsampled_signal_length = int(np.ceil(source_signal.size / downsample_ratio))
            downsampled_signal = resample(source_signal, downsampled_signal_length)
            values = downsampled_signal.item()

        elif sampling_rate < STATIC_TGT_SAMPLING_RATE:
            source_signal = np.array(values)
            upsample_ratio = STATIC_TGT_SAMPLING_RATE // sampling_rate
            upsampled_signal_length = int(np.ceil(source_signal.size / upsample_ratio))
            upsampled_signal = resample(source_signal, upsampled_signal_length)
            values = upsampled_signal.item()

        feat_value_dict[feat_col] = values
    # print(f"feat_value_dict: {feat_value_dict}")
    for i in range(0, STATIC_TGT_SAMPLING_RATE):
        one_row_dict = template_row.copy()
        start_timestamp = template_row['timestamp']
        one_row_dict['timestamp'] = start_timestamp + (i * 250)
        for feat_col in SENSOR_COLUMNS:
            one_row_dict[feat_col] = feat_value_dict[feat_col][i]

        one_row_df = pd.DataFrame([one_row_dict])

        fresh_df = pd.concat([fresh_df, one_row_df.copy()], ignore_index=True)
    # print(f"=========fresh data arrived: \n{fresh_df.head()}")
    # 0. If dataframe is empty
    if len(org_raw_msg_df) == 0:
        print(f" == org raw_msg_df was empty. add new data, and go......")
        raw_msg_df = fresh_df.copy() # new global data
        del fresh_df
    else:
        corresponding_df = raw_msg_df[(raw_msg_df['user_id'] == msg_value['user_id']) & (raw_msg_df['label'] == msg_value['label'])]
        if len(corresponding_df) == 0:
            raw_msg_df = pd.concat([raw_msg_df, fresh_df], ignore_index=True)
        else:
            # 1. Check for duplicates based on user and timestamp
            duplicate = corresponding_df[corresponding_df['timestamp'] == msg_value['timestamp']]
            if len(duplicate) != 0:
                # drop data
                pass
            else:
                corresponding_df.sort_values(by=['timestamp'], ascending=True, inplace=True)
                latest_timestamp = corresponding_df['timestamp'].iloc[-1]
                if (msg_value['timestamp'] - latest_timestamp) == 250:
                    print(f"========== Can preprocess ooooh yyyyeeey =================")

                    merged_df = pd.concat([corresponding_df[-STATIC_TGT_SAMPLING_RATE:], fresh_df], ignore_index=True)
                    data_to_process = merged_df.copy()
                    can_extract_features = True
                    raw_msg_df = pd.concat([raw_msg_df, fresh_df], ignore_index=True)

                else:
                    # print(f"========== Timestamp is not recent. Cannot merge, just add this new data")
                    raw_msg_df = pd.concat([raw_msg_df, fresh_df], ignore_index=True)


    if can_extract_features:
        print(f"----------- This data will go to pre-process: --------------")
        # print(f"{data_to_process.head(8)}")
    return can_extract_features, data_to_process, raw_msg_df

def get_feature_columns():
    feature_columns = []
    feature_columns.extend(['user_id', 'label', 'timestamp', 'arrived_timestamp'])
    for s_col in SENSOR_COLUMNS:
        if s_col in ['chestAcc', "chestECG", "chestEMG", "chestEDA", "chestTemp", "chestResp"]:
            feat_types = ["mean", "std", "max", "min"]
            feature_columns.extend([f"{s_col}_{feat_type}" for feat_type in feat_types])
    return feature_columns

def extract_features(df):
    feature_dict = {
        'user_id': df['user_id'].iloc[0],
        'label': df['label'].iloc[0],
        'timestamp': df['timestamp'].iloc[0],
        'arrived_timestamp': df['arrived_timestamp'].iloc[0]
    }
    for s_col in SENSOR_COLUMNS:
        if s_col in ['chestAcc', "chestECG", "chestEMG", "chestEDA", "chestTemp", "chestResp"]:
            feature_dict[f"{s_col}_mean"] = np.mean(df[f'{s_col}'].values[:])
            feature_dict[f'{s_col}_std'] = np.std(df[f'{s_col}'].values[:])
            feature_dict[f"{s_col}_max"] = np.max(df[f'{s_col}'].values[:])
            feature_dict[f'{s_col}_min'] = np.min(df[f'{s_col}'].values[:])
        # if we want to extract diff feature for specific sensor, add here
            # else ..
    feature_df = pd.DataFrame([feature_dict])

    print(f"================ Extracted features: ============\n{feature_df.head()}")
    return feature_df

def predict_stress(feature_df):
    # Fetch model checkpoints
    model = joblib.load('./models/small_model_checkpoint.joblib')
    feat_cols = feature_df.columns.tolist()
    feat_cols = feat_cols[4:]
    print("=========== feat cols:::::::", feat_cols)
    X = feature_df[feat_cols]
    y = feature_df['label']
    print(f"===== ")
    X, y = X.values, y.values
    predicted_y = model.predict(X)
    print(f"\n-----------------------------")
    print(f"predicted_y: {predicted_y}, y: {y}, are they same? {predicted_y.item() == y}")
    print(f"\n-----------------------------")
    return predicted_y.item()


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
    msg_buf = []
    raw_msg_df = pd.DataFrame(columns=['chestAcc',  "chestECG",  "chestEMG",  "chestEDA",  "chestTemp", "chestResp",  "label", "user_id", "timestamp", "arrived_timestamp"])
    feature_columns = get_feature_columns()
    feature_buffer_df = pd.DataFrame(columns=feature_columns)

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
                # 1. Convert to readable message from bytes
                msg_value = msg.value()
                # Decode the bytes data to a string
                string_data = msg_value.decode('utf-8')
                # Load the string data into a Python dictionary
                msg_value = json.loads(string_data)
                print(f"message timestamp: {msg_value['timestamp']} ==============")

                # 2. Manage queue
                can_extract_features, data_to_process, raw_msg_df = manage_raw_data_buffer(msg_value, raw_msg_df.copy())
                if can_extract_features:
                    feature_df = extract_features(data_to_process)

                    predicted_stress_result = predict_stress(feature_df)

                    print(f"Predicted stress result is: {predicted_stress_result}")
                    # Add to feature buffer
                    feature_buffer_df = pd.concat([feature_buffer_df, feature_df], ignore_index=True)
                    print(f":::::::::::::::::::::::::: Feature buffer df: {len(feature_buffer_df)}::::::::::::::")
                """

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
                                
                """
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


#empt_msg= []
#preprocess_data(empt_msg)