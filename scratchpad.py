import pandas as pd
import time
import datetime


import time
import datetime



def add_timestamps(user_df):
    len_user_df = len(user_df)

    ts_start = time.time()
    current_datetime = datetime.datetime.fromtimestamp(ts_start)
    start_of_today = current_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_today_timestamp_millis = int(start_of_today.timestamp()) * 1000

    ts_list = [start_of_today_timestamp_millis + (250 * i) for i in range(len_user_df)]
    user_df['timestamp'] = ts_list
    # print("user_df ts dtype: ", user_df['timestamp'].)

    return user_df

def generate_4fold_data(user_df):
    len_user_df = len(user_df)

    if len_user_df % 4 != 0:
        moks = len_user_df // 4
        remainder = len_user_df - (moks * 4)
        user_df = user_df[:-remainder]
    print(f"original len: {len_user_df}, updated_len user df: {len(user_df)}")
    return user_df

if __name__ == "__main__":
    file_path = "data/wesad_chest_small_3classes.csv"
    df = pd.read_csv(file_path)
    print(df.head())
    users = df['user_id'].unique().tolist()

    _4fold_df = None
    for user in users:
        user_df = df[df['user_id']==user]
        user_df = generate_4fold_data(user_df.copy())
        _4fold_df = pd.concat([_4fold_df, user_df], ignore_index=False)

    _4fold_df.to_csv(file_path, index=False)


