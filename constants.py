'''Constants used in all project'''
import os

def create_dir(dir_path):
    if "." in os.path.basename(dir_path):
        dir_path = os.path.dirname(dir_path)
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)

FAULTS_EXCEL = r'O:\Senas_FDS\RPA\monitoring\monitoring_data\FaultsExcel'
FAULTS_LOGGING = r'O:\Senas_FDS\RPA\monitoring\monitoring_data'
PERFORMANCE_LOG = r"O:\Senas_FDS\RPA\monitoring\monitoring_data\PerformanceTracker\performance.log"
TRANSITION_MATRICES_PATH = r"O:\Senas_FDS\RPA\monitoring\monitoring_data\Models"
TEMP_DIR = r"O:\Senas_FDS\RPA\monitoring\monitoring_data\Logs\Temp"
ROOT_DIR = r"O:\Senas_FDS\RPA\monitoring\monitoring_data\Logs"

create_dir(FAULTS_EXCEL)
create_dir(FAULTS_LOGGING)
create_dir(PERFORMANCE_LOG)
create_dir(TRANSITION_MATRICES_PATH)
create_dir(TEMP_DIR)
create_dir(ROOT_DIR)

#Faults
FAULT_PER_DAUG_PEREJIMU = "Per daug perėjimų"
FAULT_NEGALIMA_ESAMA_VEIKLA = "Negalima esama veikla"
FAULT_NEGALIMAS_PEREJIMAS = "Negalimas perėjimas tarp veiklų"