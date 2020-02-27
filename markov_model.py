import pandas as pd
import numpy as np
import os
import time


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
        self.transition_matrix = None
        self.transition_matrix = None
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

    def create_transition_matrix_v2(self):
        main_st = time.process_time()
        temp_df: pd.DataFrame = self.rpa_log[
            ["processName", "jobId", "timeStamp_datetime", "timeStamp", "ActivityName"]].copy()
        temp_df["DurationFromStart"] = pd.np.nan
        temp_df["DurationBetweenActivities"] = pd.np.nan
        temp_df["NextActivity"] = pd.np.nan
        temp_df["NextActivityDurationFromStart"] = pd.np.nan
        temp_df["DurationToNextActivity"] = pd.np.nan
        # temp_df["DurationFromStart_ms"] = pd.np.nan

        '''Surenkami perėjimai tarp veiklų, suskaičiuojamas laikas tarp perėjimų
        ir laikas nuo proceso pradžios iki veiklos, pabaigos veikla pažymima FINISH'''
        for jobId in temp_df["jobId"].unique():
            mask = temp_df["jobId"] == jobId
            index = temp_df.loc[mask].index.min()
            start_time = temp_df.loc[index, "timeStamp_datetime"]
            temp_df.loc[mask, "DurationFromStart"] = (temp_df.loc[mask, "timeStamp_datetime"] - start_time).dt.seconds
            temp_df.loc[mask, "DurationBetweenActivities"] = (
                    temp_df.loc[mask, "timeStamp_datetime"] - temp_df.loc[mask, "timeStamp_datetime"].shift(
                1)).dt.microseconds.fillna(0)
            temp_df.loc[mask, "NextActivity"] = temp_df.loc[mask, "ActivityName"].shift(-1)
            temp_df.loc[mask, "NextActivityDurationFromStart"] = temp_df.loc[mask, "DurationFromStart"].shift(-1)
            temp_df.loc[mask, "DurationToNextActivity"] = temp_df.loc[mask, "DurationBetweenActivities"].shift(-1)
        temp_df["NextActivity"] = temp_df["NextActivity"].fillna("FINISH")
        ft = time.process_time()
        print(f"Pridėti perėjimo laikų ir sekančių įvykių stulpeliai. {ft - main_st} s.")

        '''DataFrame visoms veikloms suskaičiuoti. Papildomai gaunamas maksimalus veiklų pasikartojimas vienam atvejui'''
        activity_count_df = temp_df.groupby(["jobId", "ActivityName"]).agg({"ActivityName": "count"})
        activity_count_df = activity_count_df.groupby(level=["ActivityName"]).agg(
            {
                "ActivityName": [("TotalActivityCount", "sum")
                    , ("MaxCaseActivityCount", "max")]
            })
        activity_count_df.columns = activity_count_df.columns.get_level_values(1)

        '''DataFrame max perejimams tarp veiklu suskaiciuoti'''
        transition_count_df = temp_df.groupby(["jobId", "ActivityName", "NextActivity"]).agg({"ActivityName": "count"})
        transition_count_df = transition_count_df.groupby(level=["ActivityName", "NextActivity"]).agg(
            {
                "ActivityName": [("MaxCaseTransitionCount", "max")]
            })
        transition_count_df.columns = transition_count_df.columns.get_level_values(1)

        self.transition_matrix = temp_df.groupby(["ActivityName", "NextActivity"]) \
            .agg({
            "DurationFromStart": [("DurationFromStartMax", "max")
                , ("DurationFromStartMean", "mean")]
            , "NextActivityDurationFromStart": [("NextActivityDurationFromStartMax", "max")
                , ("NextActivityDurationFromStartMean", "mean")]
            , "DurationToNextActivity": [("DurationToNextActivityMax", "max")
                , ("DurationToNextActivityMean", "mean")]
            , "ActivityName": [("TransitionCount", "count")]
        })
        self.transition_matrix.columns = self.transition_matrix.columns.get_level_values(1)

        self.transition_matrix = self.transition_matrix.join(activity_count_df, on=("ActivityName"))
        self.transition_matrix = self.transition_matrix.join(transition_count_df, on=("ActivityName", "NextActivity"))
        self.transition_matrix["Probability"] = self.transition_matrix["TransitionCount"] / self.transition_matrix["TotalActivityCount"]

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
            # pašalinamas paskutinis neegzistuojantis įvykis, kuris nurodo proceso sekos pabaigos
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

    def get_activity_probability_v2(self, prev_activity_name=None, cur_activity_name=None):
        '''Patikrinama ar esama arba buvusi veikla yra perėjimų matricoje
        @prev_activity_name - buvusi veikla, jeigu paduodama į funkciją be @cur_activity_name, tuomet patikrinama ar galima
        ir grąžinamos visos galimos esamos veiklos ir jų tikimybės
        @cur_activity_name - esama veikla, jeigu paduodama į funkciją be @prev_activity_name, tuomet grąžinama esamos veiklos tikimybė,
        kitu atveju grąžinama sekančios veiklos tikimybė
        jeigu veikla nerandama, tuomet veiklos tikimybė = -1

        returns: dataframe eilutė(-s) su reikšmėmis
            index: ActivityName, NextActivity - pabaigos veikla pažymima FINISH
            columns: DurationFromStartMax (s) (maksimali trukmė nuo pradžios)
                    , DurationFromStartMean (s) (vidutinė trukmė nuo pradžios)
                    , NextActivityDurationFromStartMax (s) (sekančios veiklos, nuo ActivityName, maksimali trukmė nuo pradžios)
                    , NextActivityDurationFromStartMean (s) (sekančios veiklos, nuo ActivityName, vidutinė trukmė nuo pradžios)
                    , DurationToNextActivityMax (ms) (maksimalus perėjimo laikas tarp veiklų)
                    , DurationToNextActivityMean (ms) (vidutnis perėjimo laikas tarp veiklų)
                    , MaxCaseTransitionCount (maksimalus perėjimų skačius vienam atvejui)
                    , MaxCaseActivityCount (maksimalus veiklų skaičius vienam atvejui)
                    , Probability (veiklos tikimybė)
        '''
        if prev_activity_name and not cur_activity_name:
            if prev_activity_name in self.transition_matrix.index.get_level_values("NextActivity"):
                return self.transition_matrix.loc[(slice(None), prev_activity_name)]
            else:
                return pd.DataFrame()
        elif not prev_activity_name and cur_activity_name:
            if cur_activity_name in self.transition_matrix.index.get_level_values("ActivityName"):
                return self.transition_matrix.loc[cur_activity_name]
            else:
                return pd.DataFrame()
        elif prev_activity_name and cur_activity_name:
            if prev_activity_name in self.transition_matrix.index.get_level_values("NextActivity") \
                    and cur_activity_name in self.transition_matrix.index.get_level_values("ActivityName"):
                return self.transition_matrix.loc[cur_activity_name, prev_activity_name]
            else:
                return pd.DataFrame()

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

    def load_transition_matrix(self, process_name=None, transition_matrices_path=TRANSITION_MATRICES_PATH):
        """Užkraunama istorinė perėjimų matrica, jeigu ji randama sukurta, kitu atveju sukuriama nauja

        Params:
        process_name -> proceso pavadinimas, jeigu nėra, tuomet gaunamas process_name iš rpa_log DataFrame iš kurio galima gauti process_name
        transition_matrices_path -> kelias iki saugomų perėjimo matricų (modelių)
        """
        st = time.process_time()
        if not process_name and isinstance(self.rpa_log, pd.DataFrame):
            process_name = self.rpa_log.loc[0, "processName"]
        elif not isinstance(self.rpa_log, pd.DataFrame) and not process_name:
            raise ValueError(
                "Nėra galimybės užkrauti proceso perėjimų matricos. Neinicializuotas DataFrame arba nepateiktas proceso pavadinimas")

        transition_matrix_path = os.path.join(transition_matrices_path, process_name + ".pickle")
        if os.path.isfile(transition_matrix_path):
            self.transition_matrix = pd.read_pickle(transition_matrix_path)
            ft = time.process_time()
            print(f"Užkraunta esama perėjimų matrica. Laikas {ft - st}")
        else:
            raise FileNotFoundError("Nerastas pickle failas", transition_matrix_path)

    def transition_matrix_to_xlsx(self, file_name = "test", save_folder = TRANSITION_MATRICES_PATH):
        """Save transition matrix to xlsx"""
        st = time.process_time()
        if isinstance(self.rpa_log, pd.DataFrame):
            file_name = self.rpa_log.loc[0, "processName"]
        try:
            self.transition_matrix.to_excel(os.path.join(save_folder), file_name+".xlsx")
        except Exception as e:
            print(f"nepavyko išsaugit excelio. {e}")
        ft = time.process_time()
        print(f"Perėjimų matrica išsaugota. Laikas {ft - st}")

    def transition_matrix_to_pickle(self):
        """Issaugoma perejimu matrica pickle failo fromatu (python failas), greitam jo uzkrovimui"""
        process_name = self.rpa_log.loc[0, "processName"]
        self.transition_matrix.to_pickle(os.path.join(Markov.TRANSITION_MATRICES_PATH, process_name + ".pickle"))
