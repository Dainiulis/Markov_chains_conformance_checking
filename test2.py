from transition_graph import TransitionGraph
import pandas as pd
import os

LOGS_PATH = r"D:\Dainius\Documents\_Magistro darbas data\test_data\Logs"
print(len(os.listdir(LOGS_PATH)))
x = 0
for i, file in enumerate(os.listdir(LOGS_PATH)):
    log_df = pd.read_pickle(os.path.join(LOGS_PATH, file))
    transition_graph = TransitionGraph(log_df)
    try:
        print(i, file)
        transition_graph.create_transition_graph()
        transition_graph.transition_matrix_to_pickle(folder=r"D:\Dainius\Documents\_Magistro darbas data\test_data\Models")
        x += 1
    except Exception as e:
        print(f"Failed to create transition graph. for file {file}. {e}")
print(x)