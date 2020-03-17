from enum import Enum


class Columns(Enum):
    """The most important columns"""
    CASE_ID = "CaseId"
    ACTIVITY_NAME = "ActivityName"
    TIMESTAMP_DATETIME = "TimeStamp_datetime"
    TIMESTAMP = "TimeStamp"
    PROCESS_NAME = "ProcessName"
    NEXT_ACTIVITY = "NextActivity"
    MEAN_TRANSITION_PROBABILITY_COEFFICIENT = "Mean transition probability coefficient"
    PROBABILITY = "Probability"


class Log():
    pass
