import pandas as pd
import numpy as np
import os
import sys
# sys.path.insert(0, r"O:\Senas_FDS\RPA\monitoring\Markov_chains_conformance_checking")
import time
import constants
from logs_parsing.logs import Columns
from exponential_regression import ExponentialRegression
from tkinter.filedialog import askopenfilename
from tkinter import messagebox


class TransitionGraph:
    """Klasė inicializuojama naudojant dataframe.
    Iškart sukuriama perėjimų matrica

    rpa_log_df - jau turimas log DataFrame jeigu ne DataFrame, tuomet ValueError išimtis
    """

    '''Constants'''
    POLYNOMIAL_DEGREE = 3
    INTERACTION_ONLY = False
    INCLUDE_BIAS = False

    def __init__(self, rpa_log=None):
        # init markov class
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
                raise ValueError("Not valid file path")
        else:
            """ 
            Įvykių žurnalas nepaduotas. Matrica turi būti užkraunama su tiksliu proceso pavadinimu iš pickle failo
            Užkrovimas vyksta load_transition_matrix(process_name) metodu
            """
            print("Procesas turi būti užkrautas ir nurodytas tiksliai metodui load_transition_matrix")

    def create_transition_graph(self):
        main_st = time.process_time()
        temp_df: pd.DataFrame = self.rpa_log.copy()
        if not hasattr(self, "case_count"):
            self.case_count = self.rpa_log[Columns.CASE_ID.value].unique().shape[0]
        temp_df[Columns.NEXT_ACTIVITY.value] = pd.np.nan
        temp_df["DurationBetweenActivities"] = pd.np.nan
        '''Surenkami perėjimai tarp veiklų, suskaičiuojamas laikas tarp perėjimų
        ir laikas nuo proceso pradžios iki veiklos, pabaigos veikla pažymima FINISH'''
        for jobId in temp_df[Columns.CASE_ID.value].unique():
            mask = temp_df[Columns.CASE_ID.value] == jobId
            temp_df.loc[mask, Columns.NEXT_ACTIVITY.value] = temp_df.loc[mask, Columns.ACTIVITY_NAME.value].shift(-1)
            # temp_df.loc[mask, "DurationBetweenActivities"] = \
            #     (temp_df.loc[mask, Columns.TIMESTAMP_DATETIME.value].shift(-1) -
            #      temp_df.loc[mask, Columns.TIMESTAMP_DATETIME.value]).dt.total_seconds().fillna(0)
        temp_df[Columns.NEXT_ACTIVITY.value] = temp_df[Columns.NEXT_ACTIVITY.value].fillna("FINISH")
        ft = time.process_time()
        print(f"Pridėti perėjimo laikų ir sekančių įvykių stulpeliai. {ft - main_st} s.")
        # temp_df.to_pickle(
        #     os.path.join(self.TRANSITION_MATRICES_PATH, "tarpiniai_modeliu_failai", "perejimai_suskaiciuoti.pickle"))

        '''DataFrame max perejimams tarp veiklu suskaiciuoti'''
        transition_count_df = temp_df.groupby(
            [Columns.CASE_ID.value, Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value]).agg(
            {Columns.ACTIVITY_NAME.value: [("activity_count", "count")],
             "DurationBetweenActivities": [("duration_mean", "mean")]})
        transition_count_df.columns = transition_count_df.columns.get_level_values(1)

        '''Kuriami n-tųjų tikimybių modeliai'''
        '''Skaičiuojami n-tieji perėjimai'''
        transition_count_df[Columns.NTH_TRANSITION_COUNTS.value] = transition_count_df["activity_count"].apply(
            lambda x: np.ones(x, np.int8))
        transtition_count_with_probabilities = transition_count_df.copy()
        transition_count_df = transition_count_df.groupby(
            level=[Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value]).agg(
            {
                "activity_count": [(Columns.MAX_CASE_TRANSITION_COUNT.value, "max"),
                                   ("unique_transition_count", "count")],
                "duration_mean": [("duration_between_activities_mean", "mean")]
            })
        transition_count_df.columns = transition_count_df.columns.get_level_values(1)

        transtition_count_with_probabilities = transtition_count_with_probabilities \
            .join(transition_count_df, on=(Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value))

        transtition_count_with_probabilities["ffill_zeros"] = \
            transtition_count_with_probabilities["max_case_transition_count"] - \
            transtition_count_with_probabilities["activity_count"]

        transtition_count_with_probabilities[Columns.NTH_TRANSITION_COUNTS.value] = \
            transtition_count_with_probabilities.apply(lambda x: np.pad(x[Columns.NTH_TRANSITION_COUNTS.value],
                                                                        (0, x["ffill_zeros"]), 'constant'),
                                                       axis=1)

        transtition_count_with_probabilities = transtition_count_with_probabilities.groupby(
            level=[Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value])[Columns.NTH_TRANSITION_COUNTS.value] \
            .apply(np.vstack) \
            .apply(lambda x: np.sum(x, axis=0))

        transition_count_df = transition_count_df.join(transtition_count_with_probabilities,
                                                       on=(Columns.ACTIVITY_NAME.value, Columns.NEXT_ACTIVITY.value))
        transition_count_df[Columns.NTH_PROBABILITIES.value] = \
            transition_count_df[Columns.NTH_TRANSITION_COUNTS.value] / self.case_count

        transition_count_df["x"] = transition_count_df[Columns.NTH_PROBABILITIES.value] \
            .apply(lambda x: np.arange(1, x.shape[0] + 1))

        '''Exponential decay regression'''
        print("Kuriami eksponentinės regresijos modeliai")
        exp_st = time.process_time()
        # transition_count_df.to_excel(r"Data\test.xlsx")
        transition_count_df[Columns.EXPONENTIAL_DECAY_REGRESSION_MODEL.value] = \
            transition_count_df[["x", Columns.NTH_PROBABILITIES.value]] \
                .apply(lambda x: ExponentialRegression(x["x"], x[Columns.NTH_PROBABILITIES.value]), axis=1)
        exp_ft = time.process_time()
        print(f"Sukurti eksponentinės regresijos modeliai. {exp_ft - exp_st} s")
        transition_count_df["popt"] = transition_count_df[Columns.EXPONENTIAL_DECAY_REGRESSION_MODEL.value] \
            .apply(lambda x: x.popt)
        transition_count_df["pcov"] = transition_count_df[Columns.EXPONENTIAL_DECAY_REGRESSION_MODEL.value] \
            .apply(lambda x: x.pcov)
        self.transition_matrix = transition_count_df.copy()
        print(f"Transition graph created in {time.process_time() - main_st} seconds")

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

    def load_transition_matrix(self, process_name=None, transition_matrices_path=constants.TRANSITION_MATRICES_PATH,
                               file_prefix=None):
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

    def transition_matrix_to_xlsx(self, file_name="test", save_folder=constants.TRANSITION_MATRICES_PATH, file_prefix=None):
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

    def transition_matrix_to_pickle(self, file_prefix=None, folder=constants.TRANSITION_MATRICES_PATH):
        """Issaugoma perejimu matrica pickle failo fromatu (python failas), greitam jo uzkrovimui"""
        process_name = self.rpa_log.loc[0, Columns.PROCESS_NAME.value]
        if file_prefix is not None:
            process_name = file_prefix + "_" + process_name

        self.transition_matrix.to_pickle(os.path.join(folder, process_name + ".pickle"))

if __name__ == "__main__":
    log_path = askopenfilename()
    if not log_path:
        raise Exception("Nepasirinktas joks failas")
    transition_graph = TransitionGraph(log_path)
    transition_graph.create_transition_graph()
    transition_graph.transition_matrix_to_pickle()
    transition_graph.transition_matrix_to_xlsx()
    messagebox.showinfo("Modelis sukurtas", f"Proceso {transition_graph.rpa_log[Columns.PROCESS_NAME.value][0]} modelis sukurtas kataloge: {constants.TRANSITION_MATRICES_PATH}")