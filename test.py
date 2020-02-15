from markov_model import Markov, IllegalMarkovStateException
import pandas as pd
from logs_parsing.log_loader import read_log_file_as_df

WORK_lOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\work_data.pickle"
TEST_LOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\test_data.pickle"

work_log_df = read_log_file_as_df(WORK_lOG_PATH)
test_log_df = read_log_file_as_df(TEST_LOG_PATH)

work_markov = Markov(work_log_df)
work_markov.transition_matrix_to_xlsx()

for i, row in test_log_df.iterrows():
    cur_activity_name = row["ActivityName"]
    prev_activity_name = test_df.loc[i-1, "ActivityName"]
    try:
        prob = work_markov.get_activity_probability(prev_activity_name, cur_activity_name)
        print(cur_activity_name, prob)
    except IllegalMarkovStateException:
        print(cur_activity_name, "Negalima veikla")