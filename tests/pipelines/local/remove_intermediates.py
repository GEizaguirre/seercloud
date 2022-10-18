import glob
import os

path = "/tmp/lithops/sandbox"

files = glob.glob("/".join([path, "terasort_1GB.csv_*"]))

[ os.remove(f) for f in files ]
