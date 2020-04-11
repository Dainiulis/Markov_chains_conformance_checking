import pandas as pd
import time
import os
from logs_parsing.logs import Columns


def read_uipath_log_file_as_df(data, only_executing=True, without_fatal=True, information_logs=False):
    """Užkraunamas pickle failas
    params:
    data - .pickle file path arba dataframe
    """
    st = time.process_time()
    if isinstance(data, str):
        if data.endswith(".pickle"):
            df = pd.read_pickle(data)
            print("Pickle loaded")
    elif isinstance(data, pd.DataFrame):
        df = data.copy()
        print("Loaded from DataFrame")
    else:
        if data is None:
            raise Exception(f"data not passed")
        else:
            raise Exception(f"Data type {type(data)} not supported")

    columns_to_return = [Columns.PROCESS_NAME.value,
                         Columns.CASE_ID.value,
                         Columns.TIMESTAMP_DATETIME.value,
                         Columns.TIMESTAMP.value,
                         Columns.ACTIVITY_NAME.value,
                         Columns.ROBOT_NAME.value]

    # Filtering
    if information_logs:
        # this is for information level logs
        print("Loading information logs")
        df = df[(df["level"] != "Trace")].copy()
        df["ActivityName"] = df["fileName"] + "|" + df["level"]
        columns_to_return.extend(["level", "message"])
    else:
        # this is for trace level logs
        print("Loading Trace level jobs")
        if without_fatal:
            faulted_jobIds = df.loc[df["level"] == "Fatal", "jobId"].unique()
            faulted_cases_rows = df["jobId"].isin(faulted_jobIds)
            df = df[~faulted_cases_rows].copy()
        mask = (df["level"] == "Trace") & (df["State"] != "Closed")
        if only_executing:
            mask = (df["State"] == "Executing") & mask
        df = df[mask].copy()
        df["ActivityName"] = df["DisplayName"] + "|" + df["State"] + "|" + df["fileName"]

    df.reset_index(inplace=True, drop=True)
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
    ft = time.process_time()
    print(f"Užkrautas {os.path.basename(str(data))} failas. Laikas: {ft - st}")
    return df[columns_to_return]
