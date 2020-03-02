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

main_st = perf_counter() #timestamp
print("Unikalių atvejų: ", test_log_df["jobId"].unique().shape[0])
faults = []
cur_transitions = {}
for case_id in test_log_df["jobId"].unique():
    case_mask = test_log_df["jobId"] == case_id
    next_activities_df = pd.DataFrame()
    prev_activity = ""
    df = test_log_df[case_mask].copy().reset_index(drop=True)
    case_st = df.loc[0, "timeStamp_datetime"]
    for i, row in df.iterrows():
        fault = None
        st = perf_counter()
        cur_job_id = row["jobId"]
        cur_activity_name = row["ActivityName"]
        if not next_activities_df.empty:
            if cur_activity_name not in next_activities_df.index:
                fault = [f"Negalimas perėjimas tarp veiklų."
                        , f"Buvusi veikla {prev_activity} -> esama veikla {cur_activity_name}"]
            else:
                key = (cur_job_id, prev_activity, cur_activity_name)
                if key in cur_transitions.keys():
                    cur_transitions[key] += 1
                else:
                    cur_transitions[key] = 0

                transition: pd.DataFrame = next_activities_df.loc[cur_activity_name]

                prob = transition["Probability"]
                max_transition_cnt = transition["MaxCaseTransitionCount"]
                max_case_activity_cnt = transition["MaxCaseActivityCount"]


                if prob < 0.0005:
                    faults.append([cur_job_id] + [f"Maža perėjimo tikimybė", f"{prob}"])

                if cur_transitions[key] > max_transition_cnt * 2:
                    faults.append([cur_job_id] + [f"Pastebėtas per didelis perėjimų skaičius tarp veiklų"
                             ,f"Tarp veiklų {prev_activity} ir {cur_activity_name}. Pastebėtas maksimalus {max_transition_cnt}"])
                    if cur_transitions[key] > max_transition_cnt * 5:

                        elapsed_time = (row["timeStamp_datetime"] - case_st).seconds
                        duration_from_start_max = transition["DurationFromStartMax"]
                        if elapsed_time > duration_from_start_max * 5:
                            faults.append([cur_job_id] + ["Ciklas",
                                                          f"Elapsed {elapsed_time}. MaxTime {duration_from_start_max}, Diff {elapsed_time - duration_from_start_max}"])
                            print("PASTEBĖTAS CIKLAS")
                            break

                        if elapsed_time > duration_from_start_max:
                            faults.append([cur_job_id] + ["Veikla įvyko vėliau nei numatyta",
                                                          f"Elapsed {elapsed_time}. MaxTime {duration_from_start_max}, Diff {elapsed_time - duration_from_start_max}"])

        next_activities_df = work_markov.get_activity_probability_v2(cur_activity_name=cur_activity_name)
        if next_activities_df.empty:
            fault = [f"Negalima esama veikla", cur_activity_name]
        if fault:
            faults.append([cur_job_id] + fault)
        prev_activity = cur_activity_name

print(f"Finished in {perf_counter() - main_st}")

print(len(faults))
df = pd.DataFrame(data=faults, columns=["jobId", "Klasifikatorius", "Klaida"])
df.to_excel(r"D:\Dainius\Documents\_Magistro darbas data\test_data\result_data2.xlsx", index=False)

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
