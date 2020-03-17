#imports required for parsing
import pandas as pd
import os
import json
import re
from pandas.io.json import json_normalize
import time

ROOT_DIR = r"D:\Magistrinio darbo duomenys"
os.chdir(ROOT_DIR)
class UiPathLogsParser():

    def __init__(self):
        self.logs_df = None

    def _append_log_df(self, df):
        if self.logs_df is None:
            self.logs_df = df
        else:
            self.logs_df.append(df)

    def _fill_transaction_ids(self):
        self.logs_df

    # for cikle yra sužymimi visi transactionID kiekvienai transakcijai nuo pradžos iki pabaigos
    def _save_logs_by_processes(self, process_name):
        start_time = time.process_time()
        process_mask = (df["processName"] == process_name)
        df_cpy = df[process_mask].copy()
        #df_cpy.sort_values(by=["timeStamp_datetime"], inplace=True, ascending=True)
        df_cpy.reset_index(inplace=True)
        mask = (df_cpy["transactionState"] == "Ended")
        print(process_name, df_cpy.shape, df_cpy[mask].shape)
        for i, row in df_cpy[mask].iterrows():
            transactionID = row["logF_TransactionID"]
            ended_timestamp = row["timeStamp_datetime"]
            started_index = df_cpy.loc[(df_cpy["transactionState"] == "Started") \
                                    & (df_cpy["timeStamp_datetime"] < ended_timestamp)].index.max()
            print("{0} -> {1} -> {2}".format(transactionID ,ended_timestamp, started_index), flush=True, end="\r")
            if pd.isnull(started_index):
                continue
            df_cpy.loc[started_index:i, ["logF_TransactionID", "transactionStatus"]] = transactionID, row["transactionStatus"]
            print("{0}:{1}".format(started_index,i), flush=True, end="\r")
        print("All states saved. Seconds elapsed:", (time.process_time() - start_time))
        save_dir = os.path.join(ROOT_DIR, process_name)
        if not os.path.isdir(save_dir):
            os.mkdir(save_dir)
        file_path = os.path.join(save_dir, r"{0}_logs_wih_transactionIDs.csv".format(process_name))
        df_cpy["completed_in"] = (df_cpy["timeStamp_datetime"] - df_cpy["timeStamp_datetime"].shift(1)).astype('timedelta64[s]')
        df_cpy.to_csv(file_path, index=False)
        df_cpy.to_pickle(file_path+".pickle")

    # extraction of valid values
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

    def read_log_as_df(self, log_path, processName = None):
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
            start_time = time.process_time()
            print(log)
            with open(log, mode="r", encoding="utf-8") as file:
                for line in file:
                    if not line.strip():
                        continue
                    data = line[1:]
                    data = re.sub("\d+:\d+:\d+\.\d+\s\w{4,5}\s{\"", "{\"", data).strip()
                    jdata = json.loads(data)                
                    if processName:
                        if jdata["processName"] != processName:
                            continue
                    try:
                        data = self.get_valid_values(jdata)
                    except Exception as e:
                        print("Unable to parse data.", e)
                        continue
                    data_list.append(data)  
                    if i%10000 == 0:
                        print(i, flush=True, end="\r")
                    i += 1
            print("Done in", time.process_time() - start_time)
        print(i, flush=True, end="\r")
        df = pd.DataFrame(data_list)
        df["timeStamp"] = df["timeStamp"].str.replace("\+02:00", "")
        df["timeStamp_datetime"] = pd.to_datetime(df["timeStamp"])
        self._append_log_df(df)

if __name__ == "__main__":
    os.chdir(ROOT_DIR)
    folders = os.listdir()
    log_files_path = []
    for folder in folders:
        if os.path.isfile(folder):
            continue
        log_path = [os.path.join(folder, f) for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        if log_files_path:
            log_files_path += log_path
        else:
            log_files_path = log_path
    print(len(log_files_path))

    for f in log_files_path:
        if os.path.isfile(f.replace(".log", "") + ".pickle") or not f.endswith(".log"):
            continue
        df = read_log_as_df(f)
        df.to_pickle(f.replace(".log", "") + ".pickle")