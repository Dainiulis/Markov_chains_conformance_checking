import pandas as pd
import numpy as np
import os
import json
import re
from pandas.io.json import json_normalize
import time
from logs_parsing.log_loader import read_log_file_as_df


class IllegalMarkovStateException(Exception):
    """Išimtis, kuomet esama būsena nerasta perėjimų matricoje"""
    pass


class Markov():
    """Klasė inicializuojama naudojant dataframe.
    Iškart sukuriama perėjimų matrica

    rpa_log_df - jau turimas log DataFrame jeigu ne DataFrame, tuomet ValueError išimtis
    """

    TRANSITION_MATRICES_PATH = r"Data"

    def __init__(self, rpa_log_df):
        # init markov class
        if isinstance(rpa_log_df, pd.DataFrame):
            self.rpa_log = rpa_log_df
        elif rpa_log_df:
            raise ValueError("Not pandas DataFrame")
        else:
            """ 
            Įvykių žurnalas nepaduotas. Matrica turi būti užkraunama su tiksliu proceso pavadinimu iš pickle failo
            Užkrovimas vyksta load_transition_matrix(process_name) metodu
            """
            pass

    def create_transition_matrix(self):
        # Create datatable with unique activity names as index
        markov_df = self.rpa_log["ActivityName"].unique()
        markov_df = pd.DataFrame(index=markov_df, columns=["NextStates"])

        # sukuriama perėjimų matrica iš visų galimų įvykių (tiek eilutės, tiek stulpeliai)
        self.transition_matrix = pd.DataFrame(0, index=markov_df.index, columns=markov_df.index)

        st = time.process_time()
        # Surandamos būsenų poros iš originalaus DataFrame
        for state, _ in markov_df.iterrows():
            all_states_mask = self.rpa_log["ActivityName"] == state
            # Surandami sekančių įvykių indeksai
            next_states_indexes = self.rpa_log[all_states_mask].index + 1
            # Surandami visi sekantys indeksai iš dataframe ir konvertuojami į sąrašą
            next_states_list = self.rpa_log.loc[next_states_indexes, "ActivityName"]
            # pašalinamas paskutinis neegzistuojantis įvykis, kuris nurodo proceso sekos pabaigos būseną
            next_states_list.dropna(inplace=True)
            # visos rastos būsenos išsaugomos eilutėje
            markov_df.loc[state, "NextStates"] = np.array(next_states_list)
        ft = time.process_time()
        print("Surasti sekančių būsenų sąrašai. Laikas:", ft - st)

        st = time.process_time()
        # Suskaičiuojamos kiekvienos eilutės tikimybės
        for state, _ in self.transition_matrix.iterrows():
            # randami markov_df įvykių (eilučių) sąrašai
            list_states = np.array(markov_df.loc[state, "NextStates"])
            # kiekvienam stulpeliui iš sąrašo priskiriama tikimybė, kuri yra apskaičiuojama
            # visų pasikartojančių įvykių sumą dalinant iš bendros rastų įvykių sumos
            state_cols, counts = np.unique(list_states, return_counts=True)
            for state_col, counts in zip(state_cols, counts):
                self.transition_matrix.loc[state, state_col] = counts / list_states.size
        ft = time.process_time()
        print("Suskaičiuotos kiekvienos eilutės tikimybės. Laikas:", ft - st)

    def get_activity_probability(self, prev_activity_name, cur_activity_name):
        try:
            return self.transition_matrix.loc[prev_activity_name, cur_activity_name]
        except KeyError as err:
            if err.args[0] == cur_activity_name:
                # column not found (invalid current move)
                raise IllegalMarkovStateException("Negalima esama veikla")
            elif err.args[0] == prev_activity_name:
                raise IllegalMarkovStateException("Negalima buvusi veikla")
            else:
                raise err

    def load_transition_matrix(self, process_name = None, transition_matrices_path = TRANSITION_MATRICES_PATH):
        """Užkraunama istorinė perėjimų matrica, jeigu ji randama sukurta, kitu atveju sukuriama nauja

        Params:
        process_name -> proceso pavadinimas, jeigu nėra rpa_log DataFrame iš kurio galima gauti process_name
        transition_matrices_path -> kelias iki saugomų perėjimo matricų (modelių)
        """
        st = time.process_time()
        if not process_name and isinstance(self.rpa_log, pd.DataFrame):
            process_name = self.rpa_log.loc[0, "processName"]
        elif not isinstance(self.rpa_log, pd.DataFrame) and not process_name:
            raise ValueError("Nėra galimybės užkrauti proceso perėjimų matricos. Neinicializuotas DataFrame arba nepateiktas proceso pavadinimas")

        transition_matrix_path = os.path.join(transition_matrices_path, process_name + ".pickle")
        if os.path.isfile(transition_matrix_path):
            self.transition_matrix =  pd.read_pickle(transition_matrix_path)
            ft = time.process_time()
            print(f"Užkraunta esama perėjimų matrica. Laikas {ft-st}")
        else:
            raise FileNotFoundError("Nerastas pickle failas", transition_matrix_path)

    def transition_matrix_to_xlsx(self):
        st = time.process_time()
        try:
            self.transition_matrix.to_excel("test.xlsx")
        except Exception as e:
            print(f"nepavyko išsaugit excelio. {e}")
        ft = time.process_time()
        print(f"Perėjimų matrica išsaugota. Laikas {ft-st}")

    def transition_matrix_to_pickle(self):
        process_name = self.rpa_log.loc[0, "processName"]
        self.transition_matrix.to_pickle(os.path.join(Markov.TRANSITION_MATRICES_PATH, process_name + ".pickle"))
