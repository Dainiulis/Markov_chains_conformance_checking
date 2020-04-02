from markov_model import Markov, IllegalMarkovStateException
import pandas as pd


class LoopException(Exception):
    """Išimtis, kuomet esama būsena nerasta perėjimų matricoje"""
    pass


class MarkovMonitor:

    def __init__(self, markov_model):
        self.markov_model = markov_model
        self.faults = []
        self.cur_transitions = {}
        self.next_activities_df = pd.DataFrame()
        self.prev_activity = None
        self.cur_activity_name = None
        self.cur_job_id = None
        self.case_start_time = None
        self.cur_activity_time_stamp = None

    def check_activity(self, trace: pd.Series):
        if isinstance(trace, pd.Series):
            pass
        else:
            pass

        self.set_prev_activity(trace["jobId"])
        self.cur_job_id = trace["jobId"]
        self.cur_activity_name = trace["ActivityName"]
        self._check_for_faults()
        self._set_next_activities_df()

    def set_prev_activity(self, job_id):
        if self.cur_job_id != job_id:
            self.prev_activity = None
        else:
            self.prev_activity = self.cur_activity_name

    def _append_fault_message(self, classifier, fault_message):
        self.faults.append([self.cur_job_id, classifier, fault_message])

    def _set_next_activities_df(self):
        self.next_activities_df = self.markov_model.get_transition_next_activities(
            cur_activity_name=self.cur_activity_name)

    def _check_for_faults(self):
        if not self.next_activities_df.empty:
            if self.cur_activity_name not in self.next_activities_df.index:
                self._append_fault_message(f"Negalimas perėjimas tarp veiklų.",
                                           f"Buvusi veikla {self.prev_activity} -> esama veikla {self.cur_activity_name}")
            else:
                key = (self.cur_job_id, self.prev_activity, self.cur_activity_name)
                if key in self.cur_transitions.keys():
                    self.cur_transitions[key] += 1
                else:
                    self.cur_transitions[key] = 0

                transition: pd.DataFrame = self.next_activities_df.loc[self.cur_activity_name]

                prob = transition["Probability"]
                max_transition_cnt = transition["MaxCaseTransitionCount"]
                max_case_activity_cnt = transition["MaxCaseActivityCount"]

                if prob < 0.0005:
                    self._append_fault_message(f"Maža perėjimo tikimybė", f"{prob}")

                if self.cur_transitions[key] > max_transition_cnt * 2:
                    self._append_fault_message(f"Pastebėtas per didelis perėjimų skaičius tarp veiklų",
                                               f"Tarp veiklų {self.prev_activity} ir {self.cur_activity_name}. Pastebėtas maksimalus {max_transition_cnt}")
                    if self.cur_transitions[key] > max_transition_cnt * 5:
                        maximum_duration_from_start = transition["DurationFromStartMax"]
                        self._check_elapsed_time(maximum_duration_from_start)

    def _check_elapsed_time(self, maximum_duration_from_start):

        elapsed_time = (self.cur_activity_time_stamp["timeStamp_datetime"] - self.case_start_time).seconds
        if elapsed_time > maximum_duration_from_start * 5:
            self._append_fault_message("CIKLAS",
                                       f"Elapsed {elapsed_time}. MaxTime {maximum_duration_from_start}, Diff {elapsed_time - maximum_duration_from_start}")
            print("PASTEBĖTAS CIKLAS")
            self.save_faults()
            raise LoopException("Aptiktas ciklas")

        if elapsed_time > maximum_duration_from_start:
            self._append_fault_message("Veikla įvyko vėliau nei numatyta",
                                       f"Elapsed {elapsed_time}. MaxTime {maximum_duration_from_start}, Diff {elapsed_time - maximum_duration_from_start}")

    def save_faults(self):
        df = pd.DataFrame(data=self.faults, columns=["jobId", "Klasifikatorius", "Klaida"])
        df.to_excel(r"D:\Dainius\Documents\_Magistro darbas data\test_data\result_data2.xlsx", index=False)
