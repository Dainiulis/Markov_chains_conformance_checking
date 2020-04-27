import json
import re
from logs_parsing.logs import Columns
import pandas as pd

def parse_uipath_log_line(line):
    """Returns dict of parsed uipath log line.
    Returns None if unable to parse"""
    data = re.sub("\d+:\d+:\d+\.\d+\s\w{4,5}\s{\"", "{\"", line).strip()
    jdata = None
    try:
        jdata = json.loads(data)
    except Exception:
        try:
            jdata = json.loads(data[1:])
        except Exception as e:
            print(data, e)
        # raise Exception(e)
    try:
        jdata = get_valid_values(jdata)
    except Exception:
        jdata = None
    return jdata


def get_valid_values(jdata):
    display_name = ""
    state = ""
    logF_TransactionId = ""
    transactionId = ""
    transactionState = ""
    transactionStatus = ""
    file_name = ""
    message = ""
    activity = ""
    activityInfo = ""
    if "activityInfo" in jdata.keys():
        display_name = jdata["activityInfo"]["DisplayName"]
        state = jdata["activityInfo"]["State"]
        activity = jdata["activityInfo"]["Activity"]
        activityInfo = jdata["activityInfo"]
    if "logF_TransactionID" in jdata.keys():
        logF_TransactionId = jdata["logF_TransactionID"]
    if "transactionId" in jdata.keys():
        transactionId = jdata["transactionId"]
    if "transactionState" in jdata.keys():
        transactionState = jdata["transactionState"]
    if "transactionStatus" in jdata.keys():
        transactionStatus = jdata["transactionStatus"]
    if "fileName" in jdata.keys():
        file_name = jdata["fileName"]
    if "message" in jdata.keys():
        message = jdata["message"]

    return {
        "processName": jdata["processName"]
        , "DisplayName": display_name
        , "State": state
        , "Activity": activity
        , "fileName": file_name
        , "message": message
        , "fingerprint": jdata["fingerprint"]
        , "jobId": jdata["jobId"]
        , "level": jdata["level"]
        , "logF_TransactionID": logF_TransactionId
        , "timeStamp": jdata["timeStamp"]
        , "transactionId": transactionId
        , "transactionState": transactionState
        , "transactionStatus": transactionStatus
        , "robotName": jdata["robotName"]
        , "machineName": jdata["machineName"]
        , "processVersion": jdata["processVersion"]
    }


def get_uipath_log_line_for_conformance_checking(line):
    """Returns Trace level log line with display name not none
    used for conformance checking"""
    data = parse_uipath_log_line(line)
    if not data:
        return None
    if data["level"] != "Trace" or not data["DisplayName"]:
        return None
    if data["State"] not in ["Executing", "Faulted"]:
        return None
    timestamp_datetime = pd.to_datetime(re.sub("\+0[23]:00", "", data["timeStamp"]))
    return {"ActivityName": data["DisplayName"] + "|" + data["State"] + "|" + data["fileName"],
            Columns.PROCESS_NAME.value: data["processName"],
            Columns.CASE_ID.value: data["jobId"],
            Columns.TIMESTAMP.value: data["timeStamp"],
            Columns.TIMESTAMP_DATETIME.value: timestamp_datetime,
            Columns.ROBOT_NAME.value: data["robotName"],
            "processVersion": data["processVersion"]}

if __name__ == "__main__":
    with open(r"C:\Users\daini\AppData\Local\UiPath\Logs\Execution.20200426.log", mode="r", encoding="utf-8") as f:
        for line in f.readlines():
            data = get_uipath_log_line_for_conformance_checking(line)
            if data:
                print(data)
            else:
                print("None", line)



