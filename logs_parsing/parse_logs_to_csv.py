import pandas as pd
import os
import json
import re
from pandas.io.json import json_normalize

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

    data = ""
    for log in log_path:
        with open(log, "r", encoding="utf-8") as file:
            data = data + file.read()[1:]
    
    # pašalinama papildoma laiko ir log level informacija trukdanti konvertuoti į json tipą
    data = re.sub("\d{2}:\d{2}:\d{2}\.\d{4}\s\w{4,5}\s{\"", "{\"", data)

    # konvertuojamas logas į json string reprezentaciją
    data = re.sub("}\n", "},\n", data)
    data = "[{0}]".format(data[:len(data)-2])
    
    #kovertuojama string -> json -> datatable -> csv
    json_data = json.loads(data)
    #df = pd.DataFrame(json_data)
    #json normalizuojamas ir perskaitomas į datatable(daug stulpelių)
    df = json_normalize(json_data)
    return df

def convert_log_to_csv(log_path, csv_path, **kwargs):
    """
    log_path gali būti logų sąrašas (list) arba vienas failas
    gaunamas logo datatable
    DataTable gali būti filtruojamas pagal **kwargs parametrus. Key yra column_name, value yra filter value
    Tuomet išsaugomas kaip CSV tolimesnei analizei su Prom
    """
    print("STARTED...")
    df = read_log_as_df(log_path)  
    # filter by passed parameters. Key is column, value is filter value
    for col in kwargs:
        mask = df[col] == kwargs[col]
        df = df[mask]

    df.to_csv(csv_path, index=False)

    print("...DONE")


if __name__ == "__main__":
    folder = r"D:\Dainius\Documents\_Magistro darbas data\Logs_esorobot"
    log_path = os.listdir(folder)
    log_path = [os.path.join(folder, file) for file in log_path]

    log_path = r"D:\Dainius\Documents\_Magistro darbas data\Logs_esorobot\2019-12-30_Execution.log"
    csv_path = r"D:\Dainius\Documents\_Magistro darbas data\VEI_Rangovo_aktai_csv2.csv"
    convert_log_to_csv(log_path, csv_path, logF_BusinessProcessName="VEI_Rangovo_aktai")
    