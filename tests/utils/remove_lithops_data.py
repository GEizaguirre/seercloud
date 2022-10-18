from lithops import FunctionExecutor

from tests.config import cloud_config, cloud_config_tests

import subprocess

bashCommand = "rm -dr ~/.lithops/cache/lithops.runtimes/*"
process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
output, error = process.communicate()

exec = FunctionExecutor(config = cloud_config_tests())

storage = exec.storage

bucket = "seer-data2"
prefix = "lithops.runtimes"

objects = [ o["Key"] for o in storage.list_objects(bucket=bucket, prefix=prefix) ]
storage.delete_objects(bucket=bucket, key_list=objects)