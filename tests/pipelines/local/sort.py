import yaml

from seercloud.scheduler import Job
from seercloud.operation import Scan, Exchange, Sort
from tests.config import local_config

job = Job ( num_stages = 2, lithops_config = local_config())
job.add(stage = 0, op = Scan, file ="terasort_1GB.csv", bucket ="sandbox")
job.add( stage = 0, op = Exchange )
job.add( stage = 1, op = Sort, key = "c0" )
job.dependency ( parent = 0, child = 1)
job.run()