import pandas as pd
import os
import json
import re
from pandas.io.json import json_normalize

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
    if "activityInfo" in jdata.keys():
        display_name = jdata["activityInfo"]["DisplayName"]
        state = jdata["activityInfo"]["State"]
        activity = jdata["activityInfo"]["Activity"]
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
        "processName" : jdata["processName"]
        , "DisplayName" : display_name
        , "State" : state
        , "Activity" : activity
        , "fileName" : file_name
        , "message" : message
        , "fingerprint" : jdata["fingerprint"]
        , "jobId" : jdata["jobId"]
        , "level" : jdata["level"]
        , "logF_TransactionID" : logF_TransactionId
        , "timeStamp" : jdata["timeStamp"]
        , "transactionId" : transactionId
        , "transactionState" : transactionState
        , "transactionStatus" : transactionStatus
        , "robotName" : jdata["robotName"]
        , "machineName" : jdata["machineName"]
        , "processVersion" : jdata["processVersion"]
    }
    
    
def read_log_as_df(log_path):
    """
    log_path gali būti logų sąrašas (list) arba vienas failas

    Konvertuojamas logas į json
    Tuomet užkraunamas į datatable
    DataTable gali būti filtruojamas pagal **kwargs parametrus. Key yra column_name, value yra filter value
    Tuomet išsaugomas kaip CSV tolimesnei analizei su Prom
    """
    if not isinstance(log_path, list):
        #kovertuojam į list, jei tik string
        log_path = [log_path]
    data_list = []
    i = 0
    for log in log_path:
        with open(log, "r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue
                data = line[1:]
                data = re.sub("\d+:\d+:\d+\.\d+\s\w{4,5}\s{\"", "{\"", data).strip()
                jdata = json.loads(data)
                
                data = get_valid_values(jdata)
                data_list.append(data)  
                if i%10000 == 0:
                    print(i, flush=True, end="\r")
                i += 1
    print(i, flush=True, end="\r")
    df = pd.DataFrame(data_list)
    df["timeStamp"] = df["timeStamp"].str.replace("\+02:00", "")
    df["timeStampAsDate"] = pd.to_datetime(df["timeStamp"])
    return df

def rm_main(logs_path):
    df = read_log_as_df(logs_path)
    mask = df["processName"] == 'VEI_Rangovo_aktai_Prod_env'
    mask2 = ~(df["DisplayName"].isna() | (df["DisplayName"] == ""))
    new_df = df[mask & mask2].copy()
    new_df.reset_index(inplace=True)
    mask3 = (new_df["fileName"] != "Main") & (new_df["fileName"] != "GetTransactionData") & (new_df["logF_TransactionID"] != "")
    return new_df, new_df[mask3]
rm_main()