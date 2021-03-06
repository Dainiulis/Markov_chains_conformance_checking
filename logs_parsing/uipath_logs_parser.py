# imports required for parsing
import pandas as pd
import os
import sys
sys.path.append('..')
# sys.path.insert(0, r"O:\Senas_FDS\RPA\monitoring\Markov_chains_conformance_checking")
import json
import re
import constants
import time
from logs_parsing.log_loader import read_uipath_log_file_as_df, Columns
from logs_parsing.parse_uipath_log_line import get_valid_values, parse_uipath_log_line
from tkinter.filedialog import askopenfilenames
from tkinter import simpledialog
from tkinter import messagebox


class UiPathLogsParser():

    def __init__(self, log_files=[], process_name=None, save_raw=False):
        self._list_logs_df = None
        if log_files:
            if isinstance(log_files, str):
                log_files = [log_files]
        else:
            log_files = []
        self.log_files = log_files
        self.main_st = time.process_time()
        self.process_name = process_name
        self.save_raw = save_raw
        if save_raw:
            self.folder_path = os.path.join(ROOT_DIR, "LOGS_RAW")
        else:
            self.folder_path = os.path.join(ROOT_DIR, "LOGS")

    def build_logs_dataframe(self):
        if self.log_files[0].strip().endswith("pickle"):
            print("Building from pickle files")
            self._read_uipath_pickle_logs()
        else:
            print("Building from log files")
            self._read_uipath_logs_as_df_v2()

    def _append_log_df(self, df):
        if self._list_logs_df is None:
            self._list_logs_df = [df]
        else:
            self._list_logs_df.append(df)

    """
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
    """

    def _read_uipath_pickle_logs(self):
        print(f"Reading pickle files for process {process_name}")
        ms_st = time.process_time()
        for i, pickle_file in enumerate(self.log_files):
            st = time.process_time()
            pickle_df = pd.read_pickle(pickle_file)
            if self.process_name:
                mask = pickle_df["processName"] == self.process_name
                pickle_df = pickle_df[mask]
            if not hasattr(self, "logs_dataframe"):
                self.logs_dataframe = pickle_df
            else:
                self.logs_dataframe = self.logs_dataframe.append(pickle_df, ignore_index=True)
            ft = time.process_time()
            print(f"{i} file read. {os.path.basename(pickle_file)}. Elapsed time {ft-st}. Total elapsed time {ft-ms_st}")

    def _load_all_logs_to_memory(self, process_name_to_load = None):
        st = time.process_time()
        print("Loading process:", process_name_to_load)
        if process_name_to_load is not None:
            """Užkraunama tik su proceso pavadinimu"""
            pickle_files_to_load = [os.path.join(TEMP_DIR, f) for f in os.listdir(TEMP_DIR) if process_name_to_load in f]
            if pickle_files_to_load:
                self.logs_dataframe = pd.concat([pd.read_pickle(f) for f in pickle_files_to_load])
        else:
            """Užkraunami visi (nerekomenduojama)"""
            self.logs_dataframe = pd.concat(
                [pd.read_pickle(os.path.join(TEMP_DIR, f)) for f in os.listdir(TEMP_DIR)])
        for f in os.listdir(TEMP_DIR):
            if process_name_to_load in f or not process_name_to_load:
                os.remove(os.path.join(TEMP_DIR, f))

        ft = time.process_time()
        print(f"Loaded all dataframes to memory in {ft - st} seconds")

    def _parse_whole_file(self, log, process_name = None):
        print(f"Parsing whole log file {log}")
        st = time.process_time()
        with open(log, "r", encoding="utf-8") as file:
            text = file.read()
        json_text = re.sub("\d+:\d+:\d+\.\d+\s\w{4,5}\s{\"", "{\"", text).strip()
        if json_text[0] != "{":
            json_text = json_text[1:]
        df: pd.DataFrame = pd.read_json(json_text, "records", lines=True)
        mask = ~df["activityInfo"].isna()
        df.loc[mask, "DisplayName"] = df.loc[mask, "activityInfo"].apply(lambda x: x["DisplayName"])
        df.loc[mask, "State"] = df.loc[mask, "activityInfo"].apply(lambda x: x["State"])
        if process_name:
            mask = df["processName"] == process_name
            df = df[mask]
        if df.shape[0] == 0:
            ft = time.process_time()
            print(
                f"Loaded {os.path.basename(log)}. NO PROCESSES FOUND {process_name}. Elapsed time {ft - st}. Total elapsed time {ft - self.main_st}")
            return
        cols_to_choose = ["processName"
            , "DisplayName"
            , "State"
            , "Activity"
            , "fileName"
            , "message"
            , "fingerprint"
            , "jobId"
            , "level"
            , "logF_TransactionID"
            , "timeStamp"
            , "transactionId"
            , "transactionState"
            , "transactionStatus"
            , "robotName"
            , "machineName"
            , "processVersion"]
        cols_to_choose = [col for col in df.columns if col in cols_to_choose]
        df = df[cols_to_choose]
        if df["timeStamp"].dtype == object:
            df["timeStamp"] = df["timeStamp"].str.replace("\+02:00", "")
        self._save_log_as_pickle(df, log, st)
        # if not hasattr(self, "logs_dataframe"):
        #     self.logs_dataframe = df
        # else:
        #     self.logs_dataframe.append(df)

    def _save_log_as_pickle(self, df, log, st):
        #log_size_mb = os.path.getsize(log)/(1024*1024)
        log = os.path.join(TEMP_DIR, os.path.basename(os.path.dirname(log)) + '_' + os.path.basename(log.replace('.log', '.pickle')))
        df.to_pickle(log)
        ft = time.process_time()
        print(f"Saved {os.path.basename(log)}. Elapsed time {ft - st}. Total elapsed time {ft - self.main_st}. "
              f"Log shape {df.shape}")

    def _read_uipath_logs_as_df_v2(self):
        print("Building from log files (with json) parsing")
        self.main_st = time.process_time()
        if not os.path.isdir(TEMP_DIR):
            os.mkdir(TEMP_DIR)
        for log in self.log_files:
            self._parse_log_line_by_line(log)
        self._save_pickle_logs_by_processes()

    def _read_uipath_logs_as_df(self, log_path=None):
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
        for log in self.log_files:
            self.logs_dataframe = self._parse_log_line_by_line(log)

    def _parse_log_line_by_line(self, log):
        start_time = time.process_time()
        print(f"Parsing log by line " + log)
        data_list = []
        i = 0
        with open(log, mode="r", encoding="utf-8") as file:
            for line in file:
                if not line.strip():
                    continue
                if self.process_name:
                    if self.process_name not in line:
                        continue
                data = parse_uipath_log_line(line)
                if data:
                    data_list.append(data)
                else:
                    print("Failed to parse line")
                    continue
                if i % 10000 == 0:
                    print(i, flush=True, end="\r")
                i += 1
        df = pd.DataFrame(data_list)
        ft = time.process_time()
        if df.empty:
            print(f"Loaded {os.path.basename(log)}. NO PROCESSES FOUND {self.process_name}. Elapsed time {ft - start_time}. Total elapsed time {ft - self.main_st}")
            return df
        df["timeStamp"] = df["timeStamp"].str.replace("\+0[23]:00", "", regex=True)
        df["timeStamp_datetime"] = pd.to_datetime(df["timeStamp"])
        if not self.process_name:
            for unique_process_name in df["processName"].unique():
                mask = df["processName"] == unique_process_name
                save_file_name = f"{os.path.splitext(log)[0]}_{unique_process_name}{os.path.splitext(log)[1]}"
                self._save_log_as_pickle(df[mask], save_file_name, start_time)
        else:
            save_file_name = f"{os.path.splitext(log)[0]}_{self.process_name}{os.path.splitext(log)[1]}"
            self._save_log_as_pickle(df, save_file_name, start_time)
        return df

    def _save_logs_from_memory(self, process_name_to_save):
        df: pd.DataFrame = uipath_log_parser.logs_dataframe
        df.reset_index(inplace=True)

        if not self.save_raw:
            df = read_uipath_log_file_as_df(data=df, without_fatal=True, only_executing=True)
            df[Columns.TIMESTAMP.value] = df[Columns.TIMESTAMP.value].astype(str)

        if not os.path.isdir(self.folder_path):
            os.mkdir(self.folder_path)
        df.to_pickle(os.path.join(self.folder_path, f"{process_name_to_save}.pickle"))
        try:
            df.to_json(os.path.join(self.folder_path, f"{process_name_to_save}.json"), orient="records", lines=True, force_ascii=False)
        except Exception as e:
            print(f"Unable to extract to json {process_name_to_save}")

    def _save_pickle_logs_by_processes(self):
        if self.process_name is not None:
            self._load_all_logs_to_memory(self.process_name)
            self._save_logs_from_memory(self.process_name)
        else:
            files = os.listdir(TEMP_DIR)
            while files:                
                process_to_save = pd.read_pickle(os.path.join(TEMP_DIR, files[0]))
                process_to_save = process_to_save.iloc[0]["processName"]
                print("Saving process", process_to_save)
                try:
                    self._load_all_logs_to_memory(process_to_save)
                    files = os.listdir(TEMP_DIR)
                    self._save_logs_from_memory(process_to_save)
                except Exception as e:
                    print("Failed saving process logs. ", str(e))

TEMP_DIR = constants.TEMP_DIR
ROOT_DIR = constants.ROOT_DIR
os.chdir(ROOT_DIR)

if __name__ == "__main__":
    log_files_path = []
    if len(sys.argv) > 1:
        arg_folder = sys.argv[1]
        if len(sys.argv) > 2:
            process_name = sys.argv[2]
        else:
            process_name = None
        folders = os.listdir(arg_folder)
        log_files_path = []
        for root, _, files in os.walk(arg_folder):
            if not files:
                continue
            files = [os.path.join(root, f) for f in files if f.endswith(".log")]            
            if files:
                log_files_path.extend(files)
        print(len(log_files_path))
    else:
        log_files_path = askopenfilenames()
        process_name = simpledialog.askstring("UiPath žurnalų konvertavimas", "Įveskite pilną ir tikslų proceso pavadinimą arba palikite tuščią visiems procesams konvertuoti")
    # application_window = tk.Tk()
    uipath_log_parser = UiPathLogsParser(log_files_path, process_name=process_name)
    uipath_log_parser.build_logs_dataframe()
    messagebox.showinfo("UiPath žurnalų konvertavimas",
                        f"Visi žurnalai konvertuoti ir išsaugoti aplanke {uipath_log_parser.folder_path}")
