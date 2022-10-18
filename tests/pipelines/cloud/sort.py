from seercloud.scheduler import Job
from seercloud.operation import Scan, Exchange, Sort, Write
from tests.config import cloud_config, cloud_config_tests

job = Job ( num_stages = 2, lithops_config = cloud_config())
job.add(stage = 0, op = Scan, file ="terasort_1GB.csv", bucket ="seer-data2")
job.add( stage = 0, op = Exchange )
job.add( stage = 1, op = Sort, key = "0" )
job.add( stage = 1, op = Write, bucket = "seer-data2", key = "terasort_1GB_sorted")
job.dependency ( parent = 0, child = 1)
job.run()