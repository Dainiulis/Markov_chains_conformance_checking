# imports required for parsing
import pandas as pd
import os
import json
import re
from pandas.io.json import json_normalize
import time
from logs_parsing.log_loader import read_uipath_log_file_as_df

ROOT_DIR = r"D:\Magistrinio darbo duomenys"
os.chdir(ROOT_DIR)


class UiPathLogsParser():

    def __init__(self, log_files=[]):
        self.list_logs_df = None
        if log_files:
            if isinstance(log_files, str):
                log_files = [log_files]
        else:
            log_files = []
        self.log_files = log_files

    def _append_log_df(self, df):
        if self.list_logs_df is None:
            self.list_logs_df = df
        else:
            self.list_logs_df.append(df)

    def _fill_transaction_ids(self):
        self.list_logs_df

    # for cikle yra sužymimi visi transactionID kiekvienai transakcijai nuo pradžos iki pabaigos
    def _save_logs_by_processes(self, process_name):
        start_time = time.process_time()
        process_mask = (df["processName"] == process_name)
        df_cpy = df[process_mask].copy()
        # df_cpy.sort_values(by=["timeStamp_datetime"], inplace=True, ascending=True)
        df_cpy.reset_index(inplace=True)
        mask = (df_cpy["transactionState"] == "Ended")
        print(process_name, df_cpy.shape, df_cpy[mask].shape)
        for i, row in df_cpy[mask].iterrows():
            transactionID = row["logF_TransactionID"]
            ended_timestamp = row["timeStamp_datetime"]
            started_index = df_cpy.loc[(df_cpy["transactionState"] == "Started") \
                                       & (df_cpy["timeStamp_datetime"] < ended_timestamp)].index.max()
            print("{0} -> {1} -> {2}".format(transactionID, ended_timestamp, started_index), flush=True, end="\r")
            if pd.isnull(started_index):
                continue
            df_cpy.loc[started_index:i, ["logF_TransactionID", "transactionStatus"]] = transactionID, row[
                "transactionStatus"]
            print("{0}:{1}".format(started_index, i), flush=True, end="\r")
        print("All states saved. Seconds elapsed:", (time.process_time() - start_time))
        save_dir = os.path.join(ROOT_DIR, process_name)
        if not os.path.isdir(save_dir):
            os.mkdir(save_dir)
        file_path = os.path.join(save_dir, r"{0}_logs_wih_transactionIDs.csv".format(process_name))
        df_cpy["completed_in"] = (df_cpy["timeStamp_datetime"] - df_cpy["timeStamp_datetime"].shift(1)).astype(
            'timedelta64[s]')
        df_cpy.to_csv(file_path, index=False)
        df_cpy.to_pickle(file_path + ".pickle")

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

    def read_logs_as_df(self, log_path=None, process_name=None):
        """
        log_path gali būti logų sąrašas (list) arba vienas failas

        Konvertuojamas logas į json
        Tuomet užkraunamas į datatable
        DataTable gali būti filtruojamas pagal **kwargs parametrus. Key yra column_name, value yra filter value
        Tuomet išsaugomas kaip CSV tolimesnei analizei su Prom
        """
        if not self.log_files:
            if not isinstance(log_path, list) and log_path:
                # kovertuojam į list, jei tik string
                self.log_files = [log_path]
            else:
                self.log_files = log_path

        data_list = []
        i = 0
        for log in self.log_files:
            start_time = time.process_time()
            print(log)
            with open(log, mode="r", encoding="utf-8") as file:
                for line in file:
                    if not line.strip():
                        continue
                    data = line[1:]
                    data = re.sub("\d+:\d+:\d+\.\d+\s\w{4,5}\s{\"", "{\"", data).strip()
                    jdata = json.loads(data)
                    if process_name:
                        if jdata["processName"] != process_name:
                            continue
                    try:
                        data = self.get_valid_values(jdata)
                    except Exception as e:
                        print("Unable to parse data.", e)
                        continue
                    data_list.append(data)
                    if i % 10000 == 0:
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
    process_name = "NVP_Busenu_saugojimas_Prod_env"
    for folder in folders:
        if os.path.isfile(folder):
            continue
        log_path = [os.path.join(ROOT_DIR, folder, f) for f in os.listdir(folder) if os.path.isfile(os.path.join(ROOT_DIR, folder, f)) and f.endswith(".log")]
        if log_files_path:
            log_files_path += log_path
        else:
            log_files_path = log_path
    print(len(log_files_path))

    uipath_log_parser = UiPathLogsParser(log_files_path)

    uipath_log_parser.read_logs_as_df(process_name=process_name)
    df: pd.DataFrame = pd.concat(uipath_log_parser.list_logs_df, ignore_index=True)
    df.reset_index(inplace=True)
    df = read_uipath_log_file_as_df(data=df, with_faulted_cases=True)
    df.to_pickle(os.path.join(ROOT_DIR, f"{process_name}.pickle"))
    df.to_json(os.path.join(ROOT_DIR, f"{process_name}.json"))
