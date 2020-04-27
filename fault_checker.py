import pandas as pd
from transition_graph import TransitionGraph
from logs_parsing.logs import Columns
from exponential_regression import ExponentialRegression
import logging
from colorlog import ColoredFormatter
from datetime import datetime
import constants

R = 10 ** -5


def setup_logger(log_file_path =""):
    """Return a logger with a default ColoredFormatter."""
    formatter = ColoredFormatter(
        "%(log_color)s%(levelname)-8s%(reset)s %(blue)s%(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'purple',
        }
    )
    logging.basicConfig(handlers=[logging.FileHandler(log_file_path, 'w', 'utf-8')],
                        format="%(message)s"
                        )
    logger = logging.getLogger('')
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return logger


class FaultChecker:

    def __init__(self, transition_graph_model: TransitionGraph, process_name = ""):
        self.next_activities_df = pd.DataFrame()
        self.transition_graph_model = transition_graph_model
        self.transitions_counts = {}
        self.faults = []
        self.faults_dict = {}
        self.stop_checking = False
        self.faults_counter = {}
        self.faults_log_file_path = r'{2}\{0:%Y-%m-%d_%H.%M.%S}_{1}.log'.format(datetime.now(), process_name, constants.FAULTS_LOGGING)
        self.logger: logging.Logger = setup_logger(self.faults_log_file_path)

    def log_fault(self, fault_type, custom_fault_values=None, level=logging.ERROR, **extra_fault_values):
        if custom_fault_values is None:
            custom_fault_values = {}
        for key, value in extra_fault_values:
            custom_fault_values[key] = value
        if hasattr(self, "message"):
            custom_fault_values["message"] = self.message
        custom_fault_values["Klaidos tipas"] = fault_type
        custom_fault_values["Esama veikla"] = self.current_activity_name
        custom_fault_values["Buvusi veikla"] = self.previous_activity_name
        custom_fault_values[Columns.CASE_ID.value] = self.case_id
        self.faults.append(custom_fault_values.copy())
        logging.error(custom_fault_values)

        if fault_type not in self.faults_dict.keys():
            self.faults_dict[fault_type] = custom_fault_values.copy()
        #     self.faults_counter[fault_type] = 1
        # else:
        #     self.faults_counter[fault_type] += 1
        # print(f"{fault_type} - {self.faults_counter[fault_type]}")

    def save_log(self):
        if self.faults:
            df = pd.DataFrame(data=self.faults)
            df.to_excel(r"{1}\{0}.xlsx".format(self.case_id, constants.FAULTS_EXCEL),
                        index=False)

    def get_fault_counts(self):
        fault_counts = {constants.FAULT_NEGALIMA_ESAMA_VEIKLA: 0,
                        constants.FAULT_NEGALIMAS_PEREJIMAS: 0,
                        constants.FAULT_PER_DAUG_PEREJIMU: 0}
        if self.faults:
            df = pd.DataFrame(data=self.faults)
            fault_counts[constants.FAULT_NEGALIMA_ESAMA_VEIKLA] = len(df[df["Klaidos tipas"] == constants.FAULT_NEGALIMA_ESAMA_VEIKLA])
            fault_counts[constants.FAULT_NEGALIMAS_PEREJIMAS] = len(df[df["Klaidos tipas"] == constants.FAULT_NEGALIMAS_PEREJIMAS])
            fault_counts[constants.FAULT_PER_DAUG_PEREJIMU] = len(df[df["Klaidos tipas"] == constants.FAULT_PER_DAUG_PEREJIMU])
        return fault_counts

    def check_faults(self, activity_row):
        """
        Naudojamas tik šis metodas
        """
        if isinstance(activity_row, dict):
            activity_row = pd.Series(activity_row)
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
        if "message" in activity_row.index:
            self.message = activity_row["message"]

    def _set_next_activities_df(self):
        self.next_activities_df = self.transition_graph_model.get_transition_next_activities(
            cur_activity_name=self.current_activity_name)

    def _check_current_row_in_next_activities_df(self):
        """
        Šis metodas iškviečiamas, kuomet turimi galimi perėjimai pagal praeitą įvykį.
        Patikrinama įvairiomis taisyklėmis, užfiksuojamos klaidos
        """
        # jeigu nerandama perėjimuose pagal praeitą įvįkį
        if self.current_activity_name not in self.next_activities_df.index:
            self.log_fault(constants.FAULT_NEGALIMAS_PEREJIMAS, level=logging.WARNING)
        else:
            # jeigu randama, tuomet tikrinama pagal kelis būdus
            key = (self.previous_activity_name, self.current_activity_name)
            try:
                self.transitions_counts[key] += 1
            except KeyError:
                self.transitions_counts[key] = 1
            self.transition_count = self.transitions_counts[key]
            self._check_transition_faults()

    def _check_transition_faults(self):
        def __predict_nth_probability_regression():
            exponential_regression_model: ExponentialRegression = transition[Columns.EXPONENTIAL_DECAY_REGRESSION_MODEL.value]
            probability_prediction = exponential_regression_model.predict(self.transition_count)
            if probability_prediction <= R:
                custom_fault_values[Columns.PROBABILITY.value] = probability_prediction
                custom_fault_values[Columns.MAX_CASE_TRANSITION_COUNT.value] = transition[
                    Columns.MAX_CASE_TRANSITION_COUNT.value]
                self.log_fault(constants.FAULT_PER_DAUG_PEREJIMU, level=logging.ERROR, custom_fault_values=custom_fault_values)

        transition: pd.DataFrame = self.next_activities_df.loc[self.current_activity_name]
        '''Transition row'''
        # probability = transition[Columns.PROBABILITY.value]
        # max_case_activity_cnt = transition["MaxCaseActivityCount"]
        custom_fault_values = {"Perėjimo skaičius": self.transition_count
                               # "Perėjimo tikimybė": probability
                               }
        __predict_nth_probability_regression()
        # __check_logarithmic_model()
        # __check_polynomial_regression_prob()
        # __check_linear_prob()
        # __check_prob()
        # __check_transition_count()
        # __check_transition_time()
        # __check_nth_probability()
        # if hasattr(self, "polynomial_fault_counter") \
        #         and hasattr(self, "linear_fault_counter") \
        #         and hasattr(self, "logarithmic_fault_counter"):
        #     self.stop_checking = self.polynomial_fault_counter > 10 and \
        #                          self.linear_fault_counter > 10 and \
        #                          self.logarithmic_fault_counter > 10

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
            self.log_fault(constants.FAULT_NEGALIMA_ESAMA_VEIKLA, level=logging.INFO)
