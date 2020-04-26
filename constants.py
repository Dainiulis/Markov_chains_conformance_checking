'''Constants used in all project'''
import os

def create_dir(dir_path):
    if os.path.isfile(dir_path):
        dir_path = os.path.dirname(dir_path)
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)

FAULTS_EXCEL = r'D:\FaultsLogging\FaultsExcel'
FAULTS_LOGGING = r'D:\FaultsLogging'
PERFORMANCE_LOG = r"D:\FaultsLogging\PerformanceTracker\performance.log"
TRANSITION_MATRICES_PATH = r"D:\Models"
TEMP_DIR = r"D:\Logs\Temp"
ROOT_DIR = r"D:\Logs"

create_dir(FAULTS_EXCEL)
create_dir(FAULTS_LOGGING)
create_dir(PERFORMANCE_LOG)
create_dir(TRANSITION_MATRICES_PATH)
create_dir(TEMP_DIR)
create_dir(ROOT_DIR)
