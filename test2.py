from markov_model import Markov, IllegalMarkovStateException
from logs_parsing.log_loader import read_uipath_log_file_as_df
from time import perf_counter
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from logs_parsing.logs import Columns
from fault_checker import FaultChecker
import os

LOGS_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\Logs"
for file in os.listdir(LOGS_PATH):
    log_df = pd.read_pickle(os.path.join(LOGS_PATH, file))
    markov = Markov(log_df)
    try:
        markov.create_transition_graph()
        markov.transition_matrix_to_pickle(folder=r"D:\Dainius\Documents\_Magistro darbas data\test_data\Models")
    except Exception as e:
        print(e)
        print(f"Failed to create transition graph. for file {file}")