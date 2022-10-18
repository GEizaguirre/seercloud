import re

from lithops.constants import LITHOPS_TEMP_DIR
import os
import glob

log_dir = os.path.join(LITHOPS_TEMP_DIR, "logs/*")

list_of_files = glob.glob(log_dir)
latest_file = max(list_of_files, key=os.path.getctime)
with open(latest_file, "r") as f:
    log_text = f.read()


# Extract each tasks data
# logs = re.findall(r'Activation*finished\n]', log_text)
logs = log_text.split("Activation")[1:]
logs = [ "".join(["Activation", l]) for l in logs ]
# Get stage & task per log

sorted_logs = {}

for l in logs:
    stage = re.findall(r'Stage \d+', l)[0]
    stage_i = int(stage.split(" ")[1])
    task = re.findall(r'task \d+', l)[0]
    task_i = int(task.split(" ")[1])

    sorted_logs[(stage_i, task_i)] = l

sorted_logs = [ l[1] for l in sorted(sorted_logs.items()) ]
print("".join(sorted_logs))










