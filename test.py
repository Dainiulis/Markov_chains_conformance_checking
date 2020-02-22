from markov_model import Markov, IllegalMarkovStateException
from logs_parsing.log_loader import read_log_file_as_df

WORK_lOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\work_data.pickle"
TEST_LOG_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\test_data.pickle"

work_log_df = read_log_file_as_df(WORK_lOG_PATH)

work_markov = Markov(work_log_df)
try:
    work_markov.load_transition_matrix()
except ValueError:
    work_markov.create_transition_matrix()
    work_markov.transition_matrix_to_pickle()

work_markov.transition_matrix_to_xlsx()


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
