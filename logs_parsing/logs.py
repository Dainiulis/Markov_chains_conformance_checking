from enum import Enum


class Columns(Enum):
    """The most important columns"""
    CASE_ID = "CaseId"
    ACTIVITY_NAME = "ActivityName"
    TIMESTAMP_DATETIME = "TimeStamp_datetime"
    TIMESTAMP = "TimeStamp"
    PROCESS_NAME = "ProcessName"
    NEXT_ACTIVITY = "NextActivity"
    PROBABILITY = "Probability"
    NTH_PROBABILITIES = "nth_probabilities"
    NTH_TRANSITION_COUNTS = "nth_transition_counts"
    PROBABILITIES = "Probabilities"
    EXPONENTIAL_DECAY_REGRESSION_MODEL = "exponential_regression_model"
    MAX_CASE_TRANSITION_COUNT = "max_case_transition_count"
    ROBOT_NAME = "robotName"


class Log():
    pass
