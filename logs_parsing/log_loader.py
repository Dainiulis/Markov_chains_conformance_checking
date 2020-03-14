import pandas as pd
import time
import os
from logs_parsing.logs import Columns


def read_uipath_log_file_as_df(log_file_path):
    """Užkraunamas pickle failas"""
    st = time.process_time()
    if log_file_path.endswith(".pickle"):
        df = pd.read_pickle(log_file_path)
    # Filtering
    mask = df["State"] == "Executing"
    df = df[mask]
    df["ActivityName"] = df["DisplayName"] + "|" + df["State"] + "|" + df["fileName"]

    df.reset_index(inplace=True, drop=True)
    ft = time.process_time()
    df.rename(columns={
        "processName": Columns.PROCESS_NAME.value,
        "jobId": Columns.CASE_ID.value,
        "timeStamp_datetime": Columns.TIMESTAMP_DATETIME.value,
        "timeStamp": Columns.TIMESTAMP.value,
        "ActivityName": Columns.ACTIVITY_NAME.value
    }, inplace=True)
    print(f"Užkrautas {os.path.basename(log_file_path)} failas. Laikas: {ft - st}")
    return df[[Columns.PROCESS_NAME.value,
               Columns.CASE_ID.value,
               Columns.TIMESTAMP_DATETIME.value,
               Columns.TIMESTAMP.value,
               Columns.ACTIVITY_NAME.value]]
