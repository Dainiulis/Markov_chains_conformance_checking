import pandas as pd
import os
import json
import re
from pandas.io.json import json_normalize
import time
from logs_parsing.log_loader import read_log_file_as_df

class IllegalMarkovState(Exception):
    """Išimtis, kuomet esama būsena nerasta perėjimų matricoje"""
    pass

class Markov():
    """Klasė inicializuojama naudojant dataframe.
    Iškart sukuriama perėjimų matrica

    rpa_log_df - jau turimas log dataframe jeigu ne Dataframe, tuomet ValueError išimtis
    """
    def __init__(self, rpa_log_df):
        #init markov class
        if isinstance(rpa_log_df, pd.Dataframe):
            self.rpa_log = rpa_log_df
        else:
            raise ValueError("Not pandas dataframe")
        
        


    def create_transition_matrix(self):
        #Create datatable with unique activity names as index
        markov_df = self.rpa_log["ActivityName"].unique()
        markov_df = pd.DataFrame(index=markov_df, columns=["NextStates"])

        #sukuriama perėjimų matrica iš visų galimų įvykių (tiek eilutės, tiek stulpeliai)
        self.transition_matrix = pd.DataFrame(0, index=markov_df.index, columns=markov_df.index)

        st = time.process_time()
        #Suskaičiuojamos kiekvienos eilutės tikimybės
        for state, _ in self.transition_matrix.iterrows():
            #randami markov_df įvykių (eilučių) sąrašai
            list_states = markov_df.loc[state, "NextStates"]
            #suskaičiojamas sąrašo ilgis
            count_of_states = len(list_states)
            #kiekvienam stulpeliui iš sąrašo priskiriama tikimybė, kuri yra apskaičiuojama
            # visų pasikartojančių įvykių sumą dalinant iš bendros rastų įvykių sumos
            for state_col in list_states:
                self.transition_matrix.loc[state, state_col] = list_states.count(state_col) / count_of_states
        ft = time.process_time()
        print(ft - st)

    def get_activity_probability(self, prev_activity_name, cur_activity_name):
        try:
            probability = self.transition_matrix.loc[prev_activity_name, cur_activity_name]
        except KeyError as err:        
            if err.args[0] == cur_activity_name:
                # column not found (invalid current move)
                raise IllegalMarkovState
            elif err.args[0] == prev_activity_name:
                pass
            else:
                raise err


"""
#jei pirmas, tuomet praleidžiame, nes nėra ką tikrinti
skip_row = True
print("index", "problema", "esama veikla", "buvusi veikla", sep="~")
for i, row in test_df.iterrows(): 
    if skip_row:
        skip_row = False
        continue
    cur_activity_name = row["ActivityName"]
    prev_activity_name = test_df.loc[i-1, "ActivityName"]
    # try:
    #     probability = transition_matrix.loc[prev_activity_name, cur_activity_name]
    # except KeyError as err:
        
    #     if err.args[0] == cur_activity_name:
    #         # column not found (invalid current move)
    #         raise AttributeError("Negalima esama veikla")
    #         #skip_row = True
    #     elif err.args[0] == prev_activity_name:
    #         pass
    #         # row index not found (invalid previous move)
    #         #print(i, "Negalima buvusi veikla", "["+cur_activity_name+"]", "["+prev_activity_name+"]", sep="~")
    #     else:
    #         raise err
    #print(probability, prev_activity_name, cur_activity_name)#
    if probability == 0:
        print(i, "veiklos tikimybė yra 0", "["+cur_activity_name+"]", "["+prev_activity_name+"]", sep="~")
"""