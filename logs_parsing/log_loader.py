import pandas as pd

def read_log_file_as_df(log_file_path):
    if log_file_path.endswith(".pickle"):
        df = pd.read_pickle(log_file_path)
    df = pd.read_excel(log_file_path, dtype={"logF_TransactionID":object})
    #Filtering
    mask = df["State"] == "Executing"
    df = df[mask]
    df["ActivityName"] = df["DisplayName"] + "|" + df["State"] + "|" + df["fileName"]
    df.sort_values(by=["timeStamp"], inplace=True, ascending=True)
    df.reset_index(inplace=True, drop=True)
    return df