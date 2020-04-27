import os
from datetime import datetime, timedelta
import constants
import shutil

def move_old_log_files(log_path):
    if os.path.isfile(log_path):
        log_path = os.path.dirname(log_path)
    print(f"Looking for old logs in {log_path}")
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    yesterday_log = os.path.join(log_path, f"Execution.{yesterday}.log")
    if os.path.isfile(yesterday_log):        
        dest_dir = os.path.join(constants.LOGS_ARCHIVE, os.environ['COMPUTERNAME'], log_path.split("\\")[2])
        dest_file = os.path.join(dest_dir, os.path.basename(yesterday_log))
        if not os.path.isdir(dest_dir):
            os.makedirs(dest_dir)
        shutil.move(yesterday_log, dest_file)
        print("Archived log to ", dest_file)
    else:
        print("Nothing found")



if __name__ == "__main__":
    log_path = r"C:\Users\esorobot\AppData\Local\UiPath\Logs\Execution.log"
    move_old_log_files(log_path)