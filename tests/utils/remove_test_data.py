from lithops import FunctionExecutor

from tests.config import cloud_config, cloud_config_tests

exec = FunctionExecutor(config = cloud_config_tests())

storage = exec.storage

bucket = "seer-data2"
prefixes = [ "terasort_1GB_", "terasort_1GB.csv_", "lithops.jobs" ]

for prefix in prefixes:
    objects = [ o["Key"] for o in storage.list_objects(bucket=bucket, prefix=prefix) ]
    storage.delete_objects(bucket=bucket, key_list=objects)