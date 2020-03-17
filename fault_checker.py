import pandas as pd
import numpy as np
from markov_model import Markov, IllegalMarkovStateException
from logs_parsing.logs import Columns
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
import copy


class FaultChecker:

    def __init__(self, markov_model: Markov):
        self.next_activities_df = pd.DataFrame()
        self.markov_model = markov_model
        self.transitions_counts = {}
        self.faults = []
        self.faults_dict = {}
        self.stop_checking = False

    def log_fault(self, fault_type, custom_fault_values=None, **extra_fault_values):
        if custom_fault_values is None:
            custom_fault_values = {}
        for key, value in extra_fault_values:
            custom_fault_values[key] = value
        custom_fault_values["Klaidos tipas"] = fault_type
        custom_fault_values["Esama veikla"] = self.current_activity_name
        custom_fault_values["Buvusi veikla"] = self.previous_activity_name
        custom_fault_values[Columns.CASE_ID.value] = self.case_id
        self.faults.append(custom_fault_values.copy())
        if fault_type not in self.faults_dict.keys():
            self.faults_dict[fault_type] = custom_fault_values.copy()

    def save_log(self):
        if self.faults:
            df = pd.DataFrame(data=self.faults)
            df.to_excel(r"D:\Dainius\Documents\_Magistro darbas data\test_data\{0}.xlsx".format(self.case_id), index=False)

    def check_faults(self, activity_row):
        """
        Naudojamas tik šis metodas
        """
        self._set_current_activity_values(activity_row)
        self._check_current_row_faults()
        self.previous_activity_name = self.current_activity_name
        self.previous_activity_timestamp_datetime = self.current_activity_timestamp_datetime

    def _set_current_activity_values(self, activity_row):
        """
        Priskiriamos klasės reikšmės pagal esamą įvykio eilutę
        Eilutė gali būti dict arba pandas series row tipo

        """
        if not hasattr(self, "current_activity_row"):
            # pirmas įvykis, todėl išsaugoma pirmojo įvykio laiko žymė
            self.case_start_timestamp = activity_row[Columns.TIMESTAMP_DATETIME.value]
            self.case_id = activity_row[Columns.CASE_ID.value]
        self.current_activity_name = activity_row[Columns.ACTIVITY_NAME.value]
        self.current_activity_timestamp_datetime = activity_row[Columns.TIMESTAMP_DATETIME.value]

    def _set_next_activities_df(self):
        self.next_activities_df = self.markov_model.get_activity_probability_v2(
            cur_activity_name=self.current_activity_name)

    def _check_current_row_in_next_activities_df(self):
        """
        Šis metodas iškviečiamas, kuomet turimi galimi perėjimai pagal praeitą įvykį.
        Patikrinama įvairiomis taisyklėmis, užfiksuojamos klaidos
        """
        # jeigu nerandama perėjimuose pagal praeitą įvįkį
        if self.current_activity_name not in self.next_activities_df.index:
            self.log_fault("Negalimas perėjimas tarp veiklų")
        else:
            # jeigu randama, tuomet tikrinama pagal kelis būdus
            key = (self.previous_activity_name, self.current_activity_name)
            if key in self.transitions_counts.keys():
                self.transition_count = self.transitions_counts[key] + 1
            else:
                self.transition_count = 1
            self.transitions_counts[key] = self.transition_count
            self._check_transition_faults()

    def _check_transition_faults(self):

        def __check_polynomial_regression_prob():
            '''Linijinės polinominės regresijos aptikimo būdas'''
            number_to_predict = np.array([self.transition_count]).reshape(-1, 1)
            y_prediction = model.predict(PolynomialFeatures(degree=self.markov_model.POLYNOMIAL_DEGREE
                                                            , include_bias=self.markov_model.INCLUDE_BIAS
                                                            , interaction_only=self.markov_model.INTERACTION_ONLY) \
                                         .fit_transform(number_to_predict))
            y_prob = y_prediction[0] * probability
            if y_prob < -mean_probability:
                custom_fault_values["Apskaičiuota n-toji perėjimo tikimybė"] = y_prob
                custom_fault_values["Vidutinio perėjimo ir tikimybės koeficientas"] = mean_probability
                self.log_fault("Galimas ciklas (polinominė regresija)", custom_fault_values)
                if hasattr(self, "polynomial_fault_counter"):
                    self.polynomial_fault_counter += 1
                else:
                    self.polynomial_fault_counter = 0

        def __check_linear_prob():
            '''Linijinės regresijos aptikimo būdas'''
            number_to_predict = np.array([self.transition_count]).reshape(-1, 1)
            y_prediction = linear_model.predict(number_to_predict)
            y_prob = y_prediction[0] * probability
            if y_prob < -probability:
                custom_fault_values["Apskaičiuota n-toji perėjimo tikimybė"] = y_prob
                custom_fault_values["Vidutinio perėjimo ir tikimybės koeficientas"] = mean_probability
                self.log_fault("Galimas ciklas (linijinė regresija)", custom_fault_values)
                if hasattr(self, "linear_fault_counter"):
                    self.linear_fault_counter += 1
                else:
                    self.linear_fault_counter = 0

        def __check_prob():
            '''Paprasta tikimybinė klaida'''
            if probability < 0.0005:
                self.log_fault("Maža perėjimo tikimybė", custom_fault_values)

        def __check_with_heuristic_rules():
            '''Euristinės taisyklės'''
            if self.transition_count > max_transition_cnt * 2:
                self.log_fault("Pastebėtas per didelis perėjimų skaičius tarp veiklų", custom_fault_values)

                if self.transition_count > max_transition_cnt * 5:
                    elapsed_time = (self.current_activity_timestamp_datetime - self.case_start_timestamp).seconds
                    duration_from_start_max = transition["DurationFromStartMax"]

                    custom_fault_values["Laikas nuo pradžios"] = elapsed_time
                    custom_fault_values["Buvęs maksimalus užfiksuotas laikas"] = duration_from_start_max
                    custom_fault_values["Laiko skirtumas"] = elapsed_time - duration_from_start_max

                    if elapsed_time > duration_from_start_max:
                        self.log_fault("Veikla įvyko vėliau nei numatyta", custom_fault_values)

                    if elapsed_time > duration_from_start_max * 5:
                        self.log_fault("Ciklas (pagal eurisines tasykles)", custom_fault_values)

        transition: pd.DataFrame = self.next_activities_df.loc[self.current_activity_name]
        '''Transition row'''
        probability = transition[Columns.PROBABILITY.value]
        mean_probability = transition[Columns.MEAN_TRANSITION_PROBABILITY_COEFFICIENT.value]
        max_transition_cnt = transition["MaxCaseTransitionCount"]
        model = transition["model"]
        linear_model = transition["linear_model"]
        # max_case_activity_cnt = transition["MaxCaseActivityCount"]
        custom_fault_values = {"Perėjimo skaičius": self.transition_count,
                               "Maksimalus perėjimų skaičius": max_transition_cnt,
                               "Perėjimo tikimybė": probability
                               }
        __check_polynomial_regression_prob()
        __check_linear_prob()
        __check_prob()
        __check_with_heuristic_rules()
        if hasattr(self, "polynomial_fault_counter") and hasattr(self, "linear_fault_counter"):
            self.stop_checking = self.polynomial_fault_counter > 10 and self.linear_fault_counter > 10

    def _check_current_row_faults(self):
        """
        Patikrinamos klaidos esamoje eilutėje
        """
        # jeigu turimi sekantys galimi perėjimai, tuomet patikrinama ar esama veikla aptinkama tuose perėjimuose
        # sekantys perėjimai, tai perėjimai surasti naudojant buvusią veiklą
        if not self.next_activities_df.empty:
            self._check_current_row_in_next_activities_df()

        # surandamos sekančios veiklos ir patikrinama ar esama veikla galima
        self._set_next_activities_df()
        if self.next_activities_df.empty:
            self.log_fault("Negalima esama veikla")
