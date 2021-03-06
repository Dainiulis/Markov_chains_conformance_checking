import pandas as pd
import numpy as np
import os
import time

from docutils.nodes import transition
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from logs_parsing.logs import Columns
from exponential_regression import ExponentialRegression


class IllegalStateException(Exception):
    """Išimtis, kuomet esama būsena nerasta perėjimų matricoje"""
    pass


class MarkovChains:
    """Klasė inicializuojama naudojant dataframe.
    Iškart sukuriama perėjimų matrica

    rpa_log_df - jau turimas log DataFrame jeigu ne DataFrame, tuomet ValueError išimtis
    """

    '''Constants'''
    TRANSITION_MATRICES_PATH = r"D:\Models"
    POLYNOMIAL_DEGREE = 3
    INTERACTION_ONLY = False
    INCLUDE_BIAS = False

    def __init__(self, rpa_log=None):
        # init RPAMonitoringModel class
        self.transition_matrix = None
        if isinstance(rpa_log, pd.DataFrame):
            self.rpa_log = rpa_log
            self.case_count = self.rpa_log[Columns.CASE_ID.value].unique().shape[0]
        elif isinstance(rpa_log, str):
            if rpa_log.lower().strip().endswith("json"):
                self.rpa_log = pd.read_json(rpa_log, orient="records", lines=True)
            elif rpa_log.lower().strip().endswith("pickle"):
                self.rpa_log = pd.read_pickle(rpa_log)
            else:
                raise ValueError("Not pandas DataFrame")
        else:
            """ 
            Įvykių žurnalas nepaduotas. Matrica turi būti užkraunama su tiksliu proceso pavadinimu iš pickle failo
            Užkrovimas vyksta load_transition_matrix(process_name) metodu
            """
            print("Procesas turi būti užkrautas ir nurodytas tiksliai metodui load_transition_matrix")

    def probability_counter(row):
        '''Pagalbinė funkcija skaičiuojant tikimybes pagal darbo vadovo pateiktas rekomendacijas'''
        transitions = row["transition_count"]
        activity_count = row["total_activity_count"]
        probabilities = []
        for i, elem in enumerate(row["nth_transition_counts"]):
            if i == 0:
                probabilities.append(elem / activity_count)
            else:
                probabilities.append(elem / row["nth_transition_counts"][i - 1])
        return np.array(probabilities)

    def create_transition_graph(self):
        main_st = time.process_time()
        temp_df: pd.DataFrame = self.rpa_log.copy()
        # temp_df["ActivityName"] = temp_df.groupby(["CaseId", "ActivityName"]).cumcount().astype(str) + \
        #                           "_" + \
        #                           temp_df["ActivityName"]
        temp_df["DurationFromStart"] = pd.np.nan
        temp_df["DurationBetweenActivities"] = pd.np.nan
        temp_df[Columns.NEXT_ACTIVITY.value] = pd.np.nan
        temp_df["NextActivityDurationFromStart"] = pd.np.nan
        temp_df["DurationToNextActivity"] = pd.np.nan
        # temp_df["DurationFromStart_ms"] = pd.np.nan

        '''Surenkami perėjimai tarp veiklų, suskaičiuojamas laikas tarp perėjimų
        ir laikas nuo proceso pradžios iki veiklos, pabaigos veikla pažymima FINISH'''
        for jobId in temp_df[Columns.CASE_ID.value].unique():
            mask = temp_df[Columns.CASE_ID.value] == jobId
            index = temp_df.loc[mask].index.min()
            start_time = temp_df.loc[index, Columns.TIMESTAMP_DATETIME.value]
            temp_df.loc[mask, "DurationFromStart"] = (
                    temp_df.loc[mask, Columns.TIMESTAMP_DATETIME.value] - start_time).dt.seconds
            temp_df.loc[mask, "DurationBetweenActivities"] = (
                    temp_df.loc[mask, Columns.TIMESTAMP_DATETIME.value] - temp_df.loc[
                mask, Columns.TIMESTAMP_DATETIME.value].shift(
                1)).dt.microseconds.fillna(0)
            temp_df.loc[mask, Columns.NEXT_ACTIVITY.value] = temp_df.loc[mask, Columns.ACTIVITY_NAME.value].shift(-1)
            temp_df.loc[mask, "NextActivityDurationFromStart"] = temp_df.loc[mask, "DurationFromStart"].shift(-1)
            temp_df.loc[mask, "DurationToNextActivity"] = temp_df.loc[mask, "DurationBetweenActivities"].shift(-1)
        temp_df[Columns.NEXT_ACTIVITY.value] = temp_df[Columns.NEXT_ACTIVITY.value].fillna("FINISH")
        ft = time.process_time()
        print(f"Pridėti perėjimo laikų ir sekančių įvykių stulpeliai. {ft - main_st} s.")
        temp_df.to_pickle(
            os.path.join(self.TRANSITION_MATRICES_PATH, "tarpiniai_modeliu_failai", "perejimai_suskaiciuoti.pickle"))
        # temp_df.to_excel(
        #     os.path.join(self.TRANSITION_MATRICES_PATH, "tarpiniai_modeliu_failai", "perejimai_suskaiciuoti.xlsx"))
        '''DataFrame visoms veikloms suskaičiuoti. Papildomai gaunamas maksimalus veiklų pasikartojimas vienam atvejui'''
        activity_count_df = temp_df.groupby([Columns.CASE_ID.value, Columns.ACTIVITY_NAME.value]).agg(
            {Columns.ACTIVITY_NAME.value: "count"})
        activity_count_df = activity_count_df.groupby(level=[Columns.ACTIVITY_NAME.value]).agg(
            {
                Columns.ACTIVITY_NAME.value: [("total_activity_count", "sum"),
                                              ("max_case_activity_count", "max"),
                                              ("min_case_activity_count", "min")]
            })
        activity_count_df.columns = activity_count_df.columns.get_level_values(1)

        '''Duration and transition count aggregation'''
        self.transition_matrix = temp_df.groupby([Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value]) \
            .agg({
            "DurationFromStart": [("DurationFromStartMax", "max"),
                                  ("DurationFromStartMean", "mean")],
            "NextActivityDurationFromStart": [("NextActivityDurationFromStartMax", "max"),
                                              ("NextActivityDurationFromStartMean", "mean")],
            "DurationToNextActivity": [("DurationToNextActivityMax", "max"),
                                       ("DurationToNextActivityMean", "mean")],
            Columns.ACTIVITY_NAME.value: [("transition_count", "count")]
        })

        self.transition_matrix.columns = self.transition_matrix.columns.get_level_values(1)
        self.transition_matrix = self.transition_matrix.join(activity_count_df, on=(Columns.ACTIVITY_NAME.value))
        self.transition_matrix[Columns.PROBABILITY.value] = self.transition_matrix["transition_count"] / \
                                                            self.transition_matrix["total_activity_count"]

        '''DataFrame max perejimams tarp veiklu suskaiciuoti'''
        transition_count_df = temp_df.groupby(
            [Columns.CASE_ID.value, Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value]).agg(
            {Columns.ACTIVITY_NAME.value: [("activity_count", "count")]})
        transition_count_df.columns = transition_count_df.columns.get_level_values(1)

        '''Kuriami n-tųjų tikimybių modeliai'''
        '''Skaičiuojami n-tieji perėjimai'''
        transition_count_df[Columns.PROBABILITIES.value] = transition_count_df["activity_count"].apply(lambda x: np.ones(x, np.int8))
        transtition_count_with_probabilities = transition_count_df.copy()
        transition_count_df = transition_count_df.groupby(
            level=[Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value]).agg(
            {
                "activity_count": [("max_case_transition_count", "max"),
                                  ("min_case_transition_count", "min"),
                                  ("unique_transition_count", "count"),
                                  ("mean_case_transition_count", "mean")]
            })
        transition_count_df.columns = transition_count_df.columns.get_level_values(1)

        transtition_count_with_probabilities = transtition_count_with_probabilities \
            .join(transition_count_df , on=(Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value))

        transtition_count_with_probabilities["ffill_zeros"] = transtition_count_with_probabilities[
                                                                  "max_case_transition_count"] - \
                                                              transtition_count_with_probabilities["activity_count"]
        transtition_count_with_probabilities[Columns.PROBABILITIES.value] = transtition_count_with_probabilities.apply(
            lambda x: np.pad(x[Columns.PROBABILITIES.value], (0, x["ffill_zeros"]), 'constant')
            , axis=1)

        transtition_count_with_probabilities = \
            transtition_count_with_probabilities.groupby(
                level=[Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value])[Columns.PROBABILITIES.value] \
                .apply(np.vstack) \
                .apply(lambda x: np.sum(x, axis=0))

        transition_count_df = transition_count_df.join(transtition_count_with_probabilities,
                                                       on=(Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value))
        transition_count_df["nth_transition_counts"] = transition_count_df[Columns.PROBABILITIES.value]
        transition_count_df["nth_probabilities"] = transition_count_df["nth_transition_counts"] / self.case_count
        transition_count_df[Columns.PROBABILITIES.value] = transition_count_df[Columns.PROBABILITIES.value] / \
                                                           transition_count_df["unique_transition_count"]

        transition_count_df = transition_count_df.join(self.transition_matrix[[Columns.PROBABILITY.value]],
                                                       on=(Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value))

        transition_count_df["probabilities_mean"] = transition_count_df[Columns.PROBABILITIES.value].apply(lambda x: x.mean())
        transition_count_df[Columns.NTH_PROBABILITIES.value] = transition_count_df[Columns.PROBABILITIES.value]

        transition_count_df["original_probabilities"] = transition_count_df[Columns.PROBABILITIES.value]
        # transition_count_df[Columns.PROBABILITIES.value] = transition_count_df[Columns.PROBABILITIES.value] * \
        #                                                    transition_count_df[Columns.PROBABILITY.value]
        transition_count_df.drop(columns=[Columns.PROBABILITY.value], inplace=True)

        transition_count_df["x"] = transition_count_df[Columns.PROBABILITIES.value] \
            .apply(lambda x: np.arange(1, x.shape[0] + 1).reshape(-1, 1))

        transition_count_df["x_3"] = transition_count_df["x"] \
            .apply(lambda x: PolynomialFeatures(degree=self.POLYNOMIAL_DEGREE,
                                                interaction_only=self.INTERACTION_ONLY,
                                                include_bias=self.INCLUDE_BIAS) \
                   .fit_transform(x))

        transition_count_df["x_2"] = transition_count_df["x"] \
            .apply(lambda x: PolynomialFeatures(degree=2,
                                                interaction_only=self.INTERACTION_ONLY,
                                                include_bias=self.INCLUDE_BIAS) \
                   .fit_transform(x))

        '''Pridedami 0, kad būtų galima apskaičiuoti regresinius modelius'''
        # mask_mean_is_one = transition_count_df["probabilities_mean"] == 1.0
        # transition_count_df.loc[mask_mean_is_one, Columns.PROBABILITIES.value] = transition_count_df.loc[
        #     mask_mean_is_one, Columns.PROBABILITIES.value].apply(lambda x: np.append(x, 0))

        # transition_count_df[Columns.NTH_PROBABILITIES.value] = transition_count_df[Columns.NTH_PROBABILITIES.value]\
        #     .apply(lambda x: np.append(x, 0))

        '''Polynomial models'''
        transition_count_df["third_degree_polynomial_model"] = transition_count_df[["x_3", Columns.PROBABILITIES.value]] \
            .apply(lambda x: LinearRegression().fit(x["x_3"], x[Columns.PROBABILITIES.value]), axis=1)

        transition_count_df["second_degree_polynomial_model"] = transition_count_df[["x_2", Columns.PROBABILITIES.value]] \
            .apply(lambda x: LinearRegression().fit(x["x_2"], x[Columns.PROBABILITIES.value]), axis=1)

        '''Linear model'''
        transition_count_df["linear_model"] = transition_count_df[["x", Columns.PROBABILITIES.value]] \
            .apply(lambda x: LinearRegression().fit(x["x"], x[Columns.PROBABILITIES.value]), axis=1)

        transition_count_df[Columns.PROBABILITIES.value + "_w0"] = transition_count_df[Columns.PROBABILITIES.value]\
            .apply(lambda x: np.append(x, 0))

        '''Logarithmic regression'''
        transition_count_df["logarithmic_model"] = transition_count_df[["x", Columns.PROBABILITIES.value + "_w0"]] \
            .apply(lambda x: ExponentialRegression(np.append(x["x"].reshape(1, -1)[0],
                                                             x[Columns.PROBABILITIES.value + "_w0"].shape[0]),
                                                   x[Columns.PROBABILITIES.value + "_w0"]),
                   axis=1)
        transition_count_df.drop(columns=[Columns.PROBABILITIES.value + "_w0"], inplace=True)




        # '''Duration and transition count aggregation'''
        # self.transition_matrix = temp_df.groupby([Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value]) \
        #     .agg({
        #     "DurationFromStart": [("DurationFromStartMax", "max"),
        #                           ("DurationFromStartMean", "mean")],
        #     "NextActivityDurationFromStart": [("NextActivityDurationFromStartMax", "max"),
        #                                       ("NextActivityDurationFromStartMean", "mean")],
        #     "DurationToNextActivity": [("DurationToNextActivityMax", "max"),
        #                                ("DurationToNextActivityMean", "mean")],
        #     Columns.ACTIVITY_NAME.value: [("transition_count", "count")]
        # })
        #
        # self.transition_matrix.columns = self.transition_matrix.columns.get_level_values(1)
        # self.transition_matrix = self.transition_matrix.join(activity_count_df, on=(Columns.ACTIVITY_NAME.value))
        # self.transition_matrix[Columns.PROBABILITY.value] = self.transition_matrix["transition_count"] / self.transition_matrix["total_activity_count"]
        self.transition_matrix = self.transition_matrix.join(transition_count_df, on=(
            Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value))

        '''Tikimybinis masyvas pagal darbo vadovo rekomendacijas'''
        self.transition_matrix["probabillity_array"] = self.transition_matrix.apply(MarkovChains.probability_counter, axis=1)
        # self.transition_matrix["probability_array_ln_model"] = self.transition_matrix[["x", "probabillity_array"]] \
        #     .apply(lambda x: LogarithmicRegression(x["x"].reshape(1, -1)[0],
        #                                            x["probabillity_array"]),
        #            axis=1)

        # Trash
        self.transition_matrix[Columns.MEAN_TRANSITION_PROBABILITY_COEFFICIENT.value] = \
            self.transition_matrix[Columns.PROBABILITY.value] * \
            self.transition_matrix["mean_case_transition_count"]

    def create_markov_transition_matrix(self):
        # Create datatable with unique activity names as index
        # self.rpa_log["ActivityName"] = self.rpa_log.groupby(["CaseId", "ActivityName"]).cumcount().astype(str) + "_" + self.rpa_log["ActivityName"]
        markov_df = self.rpa_log[Columns.ACTIVITY_NAME.value].unique()
        markov_df = pd.DataFrame(index=markov_df, columns=[Columns.NEXT_ACTIVITY.value])

        transition_matrix_index = markov_df.index.sort_values(ascending=True)
        # sukuriama perėjimų matrica iš visų galimų įvykių (tiek eilutės, tiek stulpeliai)
        self.transition_matrix = pd.DataFrame(0, index=transition_matrix_index, columns=transition_matrix_index)

        st = time.process_time()
        # Surandamos būsenų poros iš originalaus DataFrame
        for state, _ in markov_df.iterrows():
            all_states_mask = self.rpa_log[Columns.ACTIVITY_NAME.value] == state
            # Surandami sekančių įvykių indeksai
            next_states_indexes = self.rpa_log[all_states_mask].index + 1
            # Surandami visi sekantys indeksai iš dataframe ir konvertuojami į sąrašą
            next_states_list = self.rpa_log.loc[next_states_indexes, Columns.ACTIVITY_NAME.value]
            # pašalinamas paskutinis neegzistuojantis įvykis, kuris nurodo proceso sekos pabaigos
            next_states_list.dropna(inplace=True)
            # visos rastos būsenos išsaugomos eilutėje
            markov_df.loc[state, Columns.NEXT_ACTIVITY.value] = np.array(next_states_list)
        ft = time.process_time()
        print("Surasti sekančių būsenų sąrašai. Laikas:", ft - st)

        st = time.process_time()
        # Suskaičiuojamos kiekvienos eilutės tikimybės
        for state, _ in self.transition_matrix.iterrows():
            # randami markov_df įvykių (eilučių) sąrašai
            list_states = np.array(markov_df.loc[state, Columns.NEXT_ACTIVITY.value])
            # kiekvienam stulpeliui iš sąrašo priskiriama tikimybė, kuri yra apskaičiuojama
            # visų pasikartojančių įvykių sumą dalinant iš bendros rastų įvykių sumos
            state_cols, counts = np.unique(list_states, return_counts=True)
            for state_col, counts in zip(state_cols, counts):
                self.transition_matrix.loc[state, state_col] = counts / list_states.size
        ft = time.process_time()
        print("Suskaičiuotos kiekvienos eilutės tikimybės. Laikas:", ft - st)

    def get_transition_next_activities(self, prev_activity_name=None, cur_activity_name=None):
        '''
        Patikrinama ar esama arba buvusi veikla yra perėjimų matricoje
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
            jeigu įvykis nerandamas, tuomet grąžinamas tuščias dataframe
        '''
        if prev_activity_name and not cur_activity_name:
            if prev_activity_name in self.transition_matrix.index.get_level_values(Columns.NEXT_ACTIVITY.value):
                return self.transition_matrix.loc[(slice(None), prev_activity_name)]
            else:
                '''return empty data frame'''
                return pd.DataFrame()
        elif not prev_activity_name and cur_activity_name:
            if cur_activity_name in self.transition_matrix.index.get_level_values(Columns.ACTIVITY_NAME.value):
                return self.transition_matrix.loc[cur_activity_name]
            else:
                '''return empty data frame'''
                return pd.DataFrame()
        elif prev_activity_name and cur_activity_name:
            if prev_activity_name in self.transition_matrix.index.get_level_values(Columns.NEXT_ACTIVITY.value) \
                    and cur_activity_name in self.transition_matrix.index.get_level_values(Columns.ACTIVITY_NAME.value):
                return self.transition_matrix.loc[cur_activity_name, prev_activity_name]
            else:
                '''return empty data frame'''
                return pd.DataFrame()

    def get_markov_activity_probability(self, prev_activity_name, cur_activity_name):
        try:
            return self.transition_matrix.loc[prev_activity_name, cur_activity_name]
        except KeyError as err:
            if err.args[0] == cur_activity_name:
                # column not found (invalid current move)
                raise IllegalStateException("Negalima esama veikla")
            elif err.args[0] == prev_activity_name:
                raise IllegalStateException("Negalima buvusi veikla")
            else:
                raise err

    def load_transition_matrix(self, process_name=None, transition_matrices_path=TRANSITION_MATRICES_PATH, file_prefix=None):
        """Užkraunama istorinė perėjimų matrica, jeigu ji randama sukurta, kitu atveju sukuriama nauja

        Params:
        process_name -> proceso pavadinimas, jeigu nėra, tuomet gaunamas process_name iš rpa_log DataFrame iš kurio galima gauti process_name
        transition_matrices_path -> kelias iki saugomų perėjimo matricų (modelių)
        """
        st = time.process_time()
        if hasattr(self, "rpa_log"):
            if not process_name and isinstance(self.rpa_log, pd.DataFrame):
                process_name = self.rpa_log.loc[0, Columns.PROCESS_NAME.value]
            elif not isinstance(self.rpa_log, pd.DataFrame) and not process_name:
                raise ValueError(
                    "Nėra galimybės užkrauti proceso perėjimų matricos. Neinicializuotas DataFrame arba nepateiktas proceso pavadinimas")

        if file_prefix is not None:
            process_name = file_prefix + "_" + process_name
        transition_matrix_path = os.path.join(transition_matrices_path, process_name + ".pickle")
        if os.path.isfile(transition_matrix_path):
            self.transition_matrix = pd.read_pickle(transition_matrix_path)
            ft = time.process_time()
            print(f"Užkraunta esama perėjimų matrica. Laikas {ft - st}")
        else:
            raise FileNotFoundError("Nerastas pickle failas", transition_matrix_path)

    def transition_matrix_to_xlsx(self, file_name="test", save_folder=TRANSITION_MATRICES_PATH, file_prefix=None):
        """Save transition matrix to xlsx"""
        st = time.process_time()
        if isinstance(self.rpa_log, pd.DataFrame):
            file_name = self.rpa_log.loc[0, Columns.PROCESS_NAME.value]
        try:
            if file_prefix is not None:
                file_name = file_prefix + "_" + file_name
            self.transition_matrix.to_excel(os.path.join(save_folder, file_name + ".xlsx"))
        except Exception as e:
            print(f"nepavyko išsaugit excelio. {e}")
        ft = time.process_time()
        print(f"Perėjimų matrica išsaugota. Laikas {ft - st}")

    def transition_matrix_to_pickle(self, file_prefix=None, folder=TRANSITION_MATRICES_PATH):
        """Issaugoma perejimu matrica pickle failo fromatu (python failas), greitam jo uzkrovimui"""
        process_name = self.rpa_log.loc[0, Columns.PROCESS_NAME.value]
        if file_prefix is not None:
            process_name = file_prefix + "_" + process_name

        self.transition_matrix.to_pickle(os.path.join(folder, process_name + ".pickle"))
