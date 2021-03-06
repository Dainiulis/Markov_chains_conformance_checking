import os
import sys
# sys.path.insert(0, r"O:\Senas_FDS\RPA\monitoring\Markov_chains_conformance_checking")
import time
from logs_parsing.parse_uipath_log_line import get_uipath_log_line_for_conformance_checking
from transition_graph import TransitionGraph
from logs_parsing.logs import Columns
from fault_checker import FaultChecker
from datetime import datetime, timedelta
import json
import constants
import psutil
from transition_graph import TransitionGraph
import random
from helpers import move_old_log_files
from emails import send_email

def single_log_monitoring(file_path):
    print("Monitoring started...")
    last_line = 0
    model_loaded = False
    executing = True
    case_performance_time = time.perf_counter()
    case_id = ""
    process_name = ""
    process_version = ""
    event_times = []
    performance_times = []
    event_start_time = time.perf_counter()
    last_event_time = event_start_time
    robot_name = ""
    ligai_negaunamas_ivykis_counter = 0
    while executing:
        try:
            with open(file_path, mode='r', encoding='utf-8') as file:
                line_no = 0
                for line in file.readlines():
                    line_no += 1
                    if line_no == last_line + 1:
                        event_start_time = time.perf_counter()
                        last_event_time = event_start_time
                        last_line = line_no
                        data = get_uipath_log_line_for_conformance_checking(line)
                        if data and not model_loaded:
                            transition_graph = TransitionGraph()
                            case_id = data[Columns.CASE_ID.value]
                            process_name = data[Columns.PROCESS_NAME.value]
                            robot_name = data[Columns.ROBOT_NAME.value]
                            process_version = data["processVersion"]
                            try:
                                transition_graph.load_transition_matrix(process_name)
                            except FileNotFoundError:
                                return process_name
                            fault_checker = FaultChecker(transition_graph, process_name)
                            model_loaded = True
                        if data:
                            fault_checker.check_faults(data)
                            event_times.append(data[Columns.TIMESTAMP_DATETIME.value])
                        performance_times.append(time.perf_counter() - event_start_time)
                        """Stabdom jeigu paskutinis"""
                        if "execution ended\"" in line:
                            executing = False
                if time.perf_counter() - event_start_time > constants.MIN_EVENT_RECEIVE_TIME:
                    ligai_negaunamas_ivykis_counter += 1
                    send_email(["dainius.mieziunas@eso.lt", "Justina.Kasiuleviciute@eso.lt"]
                                , f"Robotas {robot_name} procesas {process_name} ilgai negaunamas įvykis"
                                , f"Ilgai negaunamas įvykis vykstančiame procese {process_name}<br>Procesą vykdo {robot_name}<br>Praėjo {int(time.perf_counter() - last_event_time)} s.")
                    event_start_time = time.perf_counter()
        except PermissionError:
            print("---------------------------------------")
            time.sleep(1)

    """CLOSE LOGGING"""
    handlers = fault_checker.logger.handlers[:]
    for handler in handlers:
        handler.close()
        fault_checker.logger.removeHandler(handler)

    """Log performance params"""
    timedeltas = [event_times[i-1]-event_times[i] for i in range(1, len(event_times))]
    average_transition_time = (sum(timedeltas, timedelta(0)) / len(timedeltas)).seconds
    performance_time = sum(performance_times) / len(performance_times)
    case_performance_time = time.perf_counter() - case_performance_time
    analysis_row = {"case_id": case_id,
                    "process_name": process_name,
                    "robot_name": robot_name,
                    "case_performance_time": case_performance_time,
                    "fault_count": len(fault_checker.faults),
                    "traces_count": last_line,
                    "average_transition_time (s)": average_transition_time / 10**6,
                    "case_average_check_time (s)": case_performance_time / last_line,
                    "average_transition_time (us)": average_transition_time,
                    "case_performance_time (us)": case_performance_time * 10**6,
                    "faults_log_path": fault_checker.faults_log_file_path,
                    "finish_time": "{0}".format(datetime.now()),
                    "performance_time": performance_time,
                    "processVersion": process_version,
                    "Ilgai negaunamas įvykis": ligai_negaunamas_ivykis_counter}
    faults_counts = fault_checker.get_fault_counts()
    analysis_row.update(faults_counts)
    try:
        with open(constants.PERFORMANCE_LOG, mode="a", encoding='utf-8') as f:
            f.write(json.dumps(analysis_row) + "\n")
    except Exception:
        time.sleep(random.random()*10)
        with open(constants.PERFORMANCE_LOG, mode="a", encoding='utf-8') as f:
            f.write(json.dumps(analysis_row) + "\n")

    fault_checker.save_log()
    print("Monitoring ended.", last_line, "{0}".format(datetime.now()))
    return "DONE"


if __name__ == "__main__":
    try:
        user_name = sys.argv[1]
    except IndexError:
        user_name = "esorobot"
    file_path = os.path.join(os.getenv('localappdata'), r'UiPath\Logs\Execution.log')
    file_path = r"C:\Users\{0}\AppData\Local\UiPath\Logs\Execution.log".format(user_name)
    time.sleep(8)
    move_old_log_files(file_path)
    result = single_log_monitoring(file_path)
    if result != "DONE":
        log_path = os.path.join(constants.ROOT_DIR, "LOGS", result + ".pickle")
        transition_graph = TransitionGraph(log_path)
        transition_graph.create_transition_graph()
        transition_graph.transition_matrix_to_pickle()
        transition_graph.transition_matrix_to_xlsx()
        print("Modelis sukurtas", f"Proceso {transition_graph.rpa_log[Columns.PROCESS_NAME.value][0]} modelis sukurtas kataloge: {constants.TRANSITION_MATRICES_PATH}")
        single_log_monitoring(file_path)
    # while True:
    #     eso_robot_logged_in = False
    #     for user in psutil.users():
    #         if user.name.lower() == user_name:
    #             time.sleep(4)
    #             single_log_monitoring(file_path)
    #             time.sleep(5)