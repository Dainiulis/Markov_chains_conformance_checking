from win32comext.bits.test.test_bits import job

from _MarkovChains import MarkovChains, IllegalStateException
from logs_parsing.log_loader import read_uipath_log_file_as_df
from time import perf_counter
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from logs_parsing.logs import Columns
from fault_checker import FaultChecker
from transition_graph import TransitionGraph

WORK_lOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\work_data.pickle"
TEST_LOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\test_data.pickle"
LOOP_LOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\loop_case.pickle"

WORK_lOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\eksperimentas\[746049]_model_data.pickle"
TEST_LOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\eksperimentas\[97854]_test_data.pickle"

# work_log_df = read_uipath_log_file_as_df(WORK_lOG_PATH, information_logs=True)
work_log_df = pd.read_pickle(WORK_lOG_PATH)
transition_graph = TransitionGraph(work_log_df)
try:
    transition_graph.load_transition_matrix()
except FileNotFoundError:
    transition_graph.create_transition_graph()
    transition_graph.transition_matrix_to_pickle()
transition_graph.transition_matrix_to_xlsx()

# test_log_df = read_uipath_log_file_as_df(TEST_LOG_PATH, only_executing=True, without_fatal=False, information_logs=True)
test_log_df = pd.read_pickle(TEST_LOG_PATH)

main_st = perf_counter() #timestamp
print("Unikalių atvejų: ", test_log_df[Columns.CASE_ID.value].unique().shape[0])
time_analysis = []
faults = []
faults2 = []
cur_transitions = {}
for x, case_id in enumerate(test_log_df[Columns.CASE_ID.value].unique()):
    case_mask = test_log_df[Columns.CASE_ID.value] == case_id
    next_activities_df = pd.DataFrame()
    prev_activity = ""
    df = test_log_df[case_mask].copy().reset_index(drop=True)

    #calculate average case transitions time
    df["transition_time"] = (df[Columns.TIMESTAMP_DATETIME.value].shift(-1) -
                               df[Columns.TIMESTAMP_DATETIME.value]).dt.microseconds.fillna(0)
    average_transition_time = df["transition_time"].mean()

    activities_check_times = []

    case_st = df.loc[0, Columns.TIMESTAMP_DATETIME.value]
    checked_lines = 0
    case_performance_time = perf_counter()
    fault_checker = FaultChecker(transition_graph)
    for i, row in df.iterrows():
        act_st = perf_counter()
        checked_lines = i
        fault_checker.check_faults(row)
        act_ft = perf_counter()
        # activities_check_times.append(act_ft-act_st)
        if fault_checker.stop_checking:
            break
    faults.extend(fault_checker.faults)
    faults2.extend(fault_checker.faults_dict.values())
    case_performance_time = perf_counter() - case_performance_time
    analysis_row = {"case_id": case_id,
                    "case_id_i": x+1,
                    "case_performance_time": case_performance_time,
                    "fault_count": len(fault_checker.faults),
                    "traces_count": df.shape[0],
                    "average_transition_time (s)": average_transition_time / 10**6,
                    "case_average_check_time (s)": case_performance_time / df.shape[0],
                    "average_transition_time (us)": average_transition_time,
                    "case_performance_time (us)": case_performance_time * 10**6}
    time_analysis.append(analysis_row)

    # df.insert(5, "Check time", activities_check_times)
    # fldr = r"D:\Dainius\Documents\_Magistro darbas data\test_data\eksperimentas\checked_dfs"
    # df.to_excel(fldr + f"\\{case_id}.xlsx")
    # print(analysis_row)
    # fault_checker.save_log()
print(f"Finished in {perf_counter() - main_st}")

print(len(faults), checked_lines)
df = pd.DataFrame(data=faults)
df.to_excel(r"D:\Dainius\Documents\_Magistro darbas data\test_data\All_info.xlsx", index=False)
df = pd.DataFrame(data=faults2)
df.to_excel(r"D:\Dainius\Documents\_Magistro darbas data\test_data\All_info2.xlsx", index=False)
df = pd.DataFrame(data=time_analysis)
df.to_excel(r"D:\Dainius\Documents\_Magistro darbas data\test_data\analysis_info.xlsx", index=False)

def test_with_manual_input():
    print("\n*********************\n")
    cur_activity_name = ""
    prev_activity_name = ""
    prob = 0

    while cur_activity_name != "exit":
        if not prev_activity_name:
            cur_activity_name = input("Įveskite pirmąją veiklą: ")
        else:
            cur_activity_name = input(f"Buvusi veikla '{prev_activity_name}', tikimybė su esama {prob}. Įveskite naują: ")
            try:
                prob = work_markov.get_markov_activity_probability(prev_activity_name, cur_activity_name)
                # if prob < 0.1:
                #     print("Retai pasitaikanti veikla", cur_activity_name, prob)
            except IllegalStateException as e:
                if str(e).lower() == "negalima esama veikla":
                    print(cur_activity_name, "Negalima veikla")
                elif str(e).lower() == "negalima buvusi veikla":
                    continue
                else:
                    raise e

        #Tai paskutinis
        prev_activity_name = cur_activity_name

def test_with_auto_input():
    test_log_df = read_uipath_log_file_as_df(TEST_LOG_PATH)
    for i, row in test_log_df.iterrows():
        if i == 0:
            continue
        cur_activity_name = row[Columns.ACTIVITY_NAME.value]
        prev_activity_name = test_log_df.loc[i-1, Columns.ACTIVITY_NAME.value]
        try:
            prob = work_markov.get_markov_activity_probability(prev_activity_name, cur_activity_name)
            # if prob < 0.1:
            #     print("Retai pasitaikanti veikla", cur_activity_name, prob)
        except IllegalStateException as e:
            if str(e).lower() == "negalima esama veikla":
                print(cur_activity_name, "Negalima veikla")
            elif str(e).lower() == "negalima buvusi veikla":
                continue
            else:
                raise e
