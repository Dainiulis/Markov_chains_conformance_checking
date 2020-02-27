from win32comext.bits.test.test_bits import job

from markov_model import Markov, IllegalMarkovStateException
from logs_parsing.log_loader import read_log_file_as_df
from time import perf_counter
import pandas as pd
import numpy as np

WORK_lOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\work_data.pickle"
TEST_LOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\test_data.pickle"
LOOP_LOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\loop_case.pickle"

work_log_df = read_log_file_as_df(WORK_lOG_PATH)

work_markov = Markov(work_log_df)
try:
    work_markov.load_transition_matrix()
except FileNotFoundError:
    work_markov.create_transition_matrix_v2()
    work_markov.transition_matrix_to_pickle()
    work_markov.transition_matrix_to_xlsx()

test_log_df = read_log_file_as_df(TEST_LOG_PATH)
main_st = perf_counter()
prev_job_id = test_log_df["jobId"].values[0]
case_st = perf_counter()
print(test_log_df["jobId"].unique().shape[0])
next_activities_df = pd.DataFrame()
prev_activity = ""
faults_by_job = {}
cur_transitions = {}
for i, row in test_log_df.iterrows():
    fault = None
    st = perf_counter()
    cur_job_id = row["jobId"]
    cur_activity_name = row["ActivityName"]
    if not next_activities_df.empty:
        if cur_activity_name not in next_activities_df.index:
            fault = f"Negalimas perėjimas tarp veiklų {prev_activity} ir {cur_activity_name}. Perėjimo tikimybė yra 0"
        else:

            if (prev_activity, cur_activity_name) in cur_transitions.keys():
                cur_transitions[(prev_activity, cur_activity_name)] += 1
            else:
                cur_transitions[(prev_activity, cur_activity_name)] = 0

            transition: pd.DataFrame = next_activities_df.loc[cur_activity_name]
            prob = transition["Probability"]
            max_transition_cnt = transition["MaxCaseTransitionCount"]
            max_case_activity_cnt = transition["MaxCaseActivityCount"]

            if prob < 0.0005:
                fault = f"Maža perėjimo iš {prev_activity} į {cur_activity_name} tikimybė: {prob}"
            if cur_transitions[(prev_activity, cur_activity_name)] > max_transition_cnt * 2:
                fault = f"Pastebėtas per didelis perėjimų skaičius tarp veiklų {(prev_activity, cur_activity_name)}. Pastebėtas maksimalus {max_transition_cnt}"

    next_activities_df = work_markov.get_activity_probability_v2(cur_activity_name=cur_activity_name)
    if next_activities_df.empty:
        next_activities_df = next_activities_df.copy()
        fault = f"{perf_counter()-st} Negalima esama veikla", cur_activity_name

    if fault:
        if cur_job_id in faults_by_job.keys():
            faults_by_job[cur_job_id].append(fault)
        else:
            faults_by_job[cur_job_id] = [fault]

    prev_activity = cur_activity_name
    '''Uzloginama kiek laiko truko patikrinti viena atveji'''
    if cur_job_id != prev_job_id:
        print(f"Atvejis {prev_job_id} truko {perf_counter() - case_st}")
        prev_job_id = cur_job_id
        case_st = perf_counter()
print(f"Finished in {perf_counter() - main_st}")

for k, v in faults_by_job.items():
    print(f"Case {k}, {len(v)} faults")


for k, v in faults_by_job.items():
    values, counts = np.unique(v, return_counts=True)
    v = dict(zip(values, counts))
    for val, count in v.items():
        print(f"Case {k} --->>> {count} -->> {val}")

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
                prob = work_markov.get_activity_probability(prev_activity_name, cur_activity_name)
                # if prob < 0.1:
                #     print("Retai pasitaikanti veikla", cur_activity_name, prob)
            except IllegalMarkovStateException as e:
                if str(e).lower() == "negalima esama veikla":
                    print(cur_activity_name, "Negalima veikla")
                elif str(e).lower() == "negalima buvusi veikla":
                    continue
                else:
                    raise e

        #Tai paskutinis
        prev_activity_name = cur_activity_name


def test_with_auto_input():
    test_log_df = read_log_file_as_df(TEST_LOG_PATH)
    for i, row in test_log_df.iterrows():
        if i == 0:
            continue
        cur_activity_name = row["ActivityName"]
        prev_activity_name = test_log_df.loc[i-1, "ActivityName"]
        try:
            prob = work_markov.get_activity_probability(prev_activity_name, cur_activity_name)
            # if prob < 0.1:
            #     print("Retai pasitaikanti veikla", cur_activity_name, prob)
        except IllegalMarkovStateException as e:
            if str(e).lower() == "negalima esama veikla":
                print(cur_activity_name, "Negalima veikla")
            elif str(e).lower() == "negalima buvusi veikla":
                continue
            else:
                raise e
