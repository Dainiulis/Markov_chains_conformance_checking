import pandas as pd
import time
import os

def read_log_file_as_df(log_file_path):
    """Užkraunamas pickle failas"""
    st = time.process_time()
    if log_file_path.endswith(".pickle"):
        df = pd.read_pickle(log_file_path)
    #Filtering
    mask = df["State"] == "Executing"
    df = df[mask]
    df["ActivityName"] = df["DisplayName"] + "|" + df["State"] + "|" + df["fileName"]
    #df.sort_values(by=["timeStamp"], inplace=True, ascending=True)
    df.reset_index(inplace=True, drop=True)
    ft = time.process_time()
    print(f"Užkrautas {os.path.basename(log_file_path)} failas. Laikas: {ft-st}")
    return df