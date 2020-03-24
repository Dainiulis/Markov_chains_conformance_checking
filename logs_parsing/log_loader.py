import pandas as pd
import time
import os
from logs_parsing.logs import Columns


def read_uipath_log_file_as_df(data, with_faulted_cases = False, all_cases = False):
    """Užkraunamas pickle failas
    params:
    data - .pickle file path arba dataframe
    """
    st = time.process_time()
    if isinstance(data, str):
        if data.endswith(".pickle"):
            df = pd.read_pickle(data)
    elif isinstance(data, pd.DataFrame):
        df = data
    else:
        if data is None:
            raise Exception(f"data not passed")
        else:
            raise Exception(f"Data type {type(data)} not supported")
    # Filtering
    mask = df["level"] == "Trace"
    if with_faulted_cases:
        mask = (df["State"] == "Executing") & mask
    elif not all_cases:
        faulted_jobIds = df.loc[df["level"] == "Fatal", "jobId"].unique()
        faulted_cases_rows = df["jobId"].isin(faulted_jobIds)
        mask = (df["State"] == "Executing") & ~faulted_cases_rows & mask
    df = df[mask].copy()
    df["ActivityName"] = df["DisplayName"] + "|" + df["State"] + "|" + df["fileName"]

    df.reset_index(inplace=True, drop=True)
    ft = time.process_time()
    df["timeStamp_datetime"] = pd.to_datetime(df["timeStamp"].str.replace("\+02:00", ""))
    df.rename(columns={
        "processName": Columns.PROCESS_NAME.value,
        "jobId": Columns.CASE_ID.value,
        "timeStamp_datetime": Columns.TIMESTAMP_DATETIME.value,
        "timeStamp": Columns.TIMESTAMP.value,
        "ActivityName": Columns.ACTIVITY_NAME.value
    }, inplace=True)
    if not isinstance(data, str):
        data = type(data)
    print(f"Užkrautas {os.path.basename(str(data))} failas. Laikas: {ft - st}")
    return df[[Columns.PROCESS_NAME.value,
               Columns.CASE_ID.value,
               Columns.TIMESTAMP_DATETIME.value,
               Columns.TIMESTAMP.value,
               Columns.ACTIVITY_NAME.value]]
