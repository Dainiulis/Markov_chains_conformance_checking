from win32comext.bits.test.test_bits import job

from markov_model import Markov, IllegalMarkovStateException
from logs_parsing.log_loader import read_uipath_log_file_as_df
from time import perf_counter
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from logs_parsing.logs import Columns
from fault_checker import FaultChecker

WORK_lOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\work_data.pickle"
TEST_LOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\test_data.pickle"
LOOP_LOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\loop_case.pickle"

work_log_df = read_uipath_log_file_as_df(WORK_lOG_PATH)

work_markov = Markov(work_log_df)

try:
    work_markov.load_transition_matrix()
except FileNotFoundError:
    work_markov.create_transition_matrix_v2()
    work_markov.transition_matrix_to_pickle()
work_markov.transition_matrix_to_xlsx()

test_log_df = read_uipath_log_file_as_df(TEST_LOG_PATH)

main_st = perf_counter() #timestamp
print("Unikalių atvejų: ", test_log_df[Columns.CASE_ID.value].unique().shape[0])
time_analysis = []
faults = []
cur_transitions = {}
for case_id in test_log_df[Columns.CASE_ID.value].unique():
    case_performance_time = perf_counter()
    case_mask = test_log_df[Columns.CASE_ID.value] == case_id
    next_activities_df = pd.DataFrame()
    prev_activity = ""
    df = test_log_df[case_mask].copy().reset_index(drop=True)
    case_st = df.loc[0, Columns.TIMESTAMP_DATETIME.value]
    checked_lines = 0
    fault_checker = FaultChecker(work_markov)
    for i, row in df.iterrows():
        checked_lines = i
        fault_checker.check_faults(row)
        continue
        '''
        st = perf_counter()
        cur_job_id = row[Columns.CASE_ID.value]
        cur_activity = row[Columns.ACTIVITY_NAME.value]
        faults_row = {}
        fault_type = None

        if not next_activities_df.empty:
            if cur_activity not in next_activities_df.index:
                fault_type = "Negalimas perėjimas tarp veiklų"
            else:
                key = (cur_job_id, prev_activity, cur_activity)

                #Priskiriamas perėjimų skaičius
                if key in cur_transitions.keys():
                    transition_was_spotted_no = cur_transitions[key] + 1
                else:
                    transition_was_spotted_no = 1

                transition: pd.DataFrame = next_activities_df.loc[cur_activity]
                #Transition row
                prob = transition["Probability"]
                max_transition_cnt = transition["MaxCaseTransitionCount"]
                model = transition["model"]
                max_case_activity_cnt = transition["MaxCaseActivityCount"]

                """
                #Linijinės regresijos aptikimo būdas
                number_to_predict = np.array([transition_was_spotted_no]).reshape(-1, 1)
                y_prediction = model.predict(PolynomialFeatures(degree=work_markov.POLYNOMIAL_DEGREE
                                                                 , include_bias=work_markov.INCLUDE_BIAS
                                                                 , interaction_only=work_markov.INTERACTION_ONLY) \
                                              .fit_transform(number_to_predict))
                y_prob = y_prediction[0] * prob
                if y_prob < -prob:
                    fault_type = "Galimas ciklas"
                    faults_row["Apskaičiuota n-toji perėjimo tikimybė"] = y_prob
                """

                #Tikimybinė klaida
                if prob < 0.0005:
                    fault_type = "Maža perėjimo tikimybė"

                #Euristinės taisyklės
                if transition_was_spotted_no > max_transition_cnt * 2:
                    fault_type = "Pastebėtas per didelis perėjimų skaičius tarp veiklų"

                    if transition_was_spotted_no > max_transition_cnt * 5:
                        elapsed_time = (row[Columns.TIMESTAMP_DATETIME.value] - case_st).seconds
                        duration_from_start_max = transition["DurationFromStartMax"]

                        if elapsed_time > duration_from_start_max:
                            fault_type = "Veikla įvyko vėliau nei numatyta"
                            faults_row["Laikas nuo pradžios"] = elapsed_time
                            faults_row["Buvęs maksimalus užfiksuotas laikas"] = duration_from_start_max
                            faults_row["Laiko skirtumas"] = elapsed_time - duration_from_start_max

                        if elapsed_time > duration_from_start_max * 5:
                            fault_type = "Ciklas (pagal eurisines tasykles)"

                #Priskiri pagal nutylejimą reikalingi klaidų parametrai
                if fault_type:
                    faults_row["Perėjimo skaičius"] = transition_was_spotted_no
                    faults_row["Maksimalus perėjimų skaičius"] = max_transition_cnt
                    faults_row["Perėjimo tikimybė"] = prob

                #Išsaugomas užfiksuotas perėjimų skaičius
                cur_transitions[key] = transition_was_spotted_no

        next_activities_df = work_markov.get_activity_probability_v2(cur_activity_name=cur_activity)

        if next_activities_df.empty:
            fault_type = "Negalima esama veikla"

        #Append fault
        if fault_type:
            faults_row[Columns.CASE_ID.name] = case_id
            faults_row["Current activity"] = cur_activity
            faults_row["Previous activity"] = prev_activity
            faults_row["Fault type"] = fault_type
            faults.append(faults_row)
        prev_activity = cur_activity
        '''
    faults.extend(fault_checker.faults)
    case_performance_time = perf_counter() - case_performance_time
    time_analysis.append({"case_id": case_id,
                          "case_performance_time": case_performance_time,
                          "fault_count": len(fault_checker.faults),
                          "traces_count": df.shape[0] })
    # fault_checker.save_log()
print(f"Finished in {perf_counter() - main_st}")

print(len(faults), checked_lines)
df = pd.DataFrame(data=faults)
df.to_excel(r"D:\Dainius\Documents\_Magistro darbas data\test_data\All2.xlsx", index=False)
df = pd.DataFrame(data=time_analysis)
df.to_excel(r"D:\Dainius\Documents\_Magistro darbas data\test_data\analysis.xlsx", index=False)

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
    test_log_df = read_uipath_log_file_as_df(TEST_LOG_PATH)
    for i, row in test_log_df.iterrows():
        if i == 0:
            continue
        cur_activity_name = row[Columns.ACTIVITY_NAME.value]
        prev_activity_name = test_log_df.loc[i-1, Columns.ACTIVITY_NAME.value]
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
