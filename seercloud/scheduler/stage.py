import logging
from typing import List

from lithops import FunctionExecutor

from seercloud.metadata import TaskInfo, DataInfo
from seercloud.operation import Operation
from seercloud.operation.Partition import Partition
from seercloud.scheduler.task import Task, run_task

logger = logging.getLogger(__name__)

class Stage():

    # Task info
    surname_in: str = None
    surname_out: str = None
    stage_id: int = None
    operations: List[Operation] = list()
    num_tasks: int = None
    job_id: int = None
    read_path: str = None
    read_bucket: str = None
    write_path: str = None
    write_bucket: str = None

    # Data info
    delimiter: str = None
    types: list = None
    approx_rows: int = None

    key: str = None

    do_kp: bool
    partition: Partition

    tasks: List[Task]

    def __init__(self, stage_id: int, job_id: int, **kwargs):

        self.__dict__.update(**kwargs)
        self.operations = list()
        self.stage_id = stage_id
        self.job_id = job_id
        self.do_kp = False
        self.partition = None


    def add_op(self, operation: Operation, **kwargs):
        self.operations.append(operation)

    def set_num_tasks(self, num_tasks: int):
        self.num_tasks = num_tasks


    def run(self, executor: FunctionExecutor):


        self.tasks = [ Task(gen_task_info(self, ti), gen_data_info(self))
                       for ti in range(self.num_tasks) ]

        for t in self.tasks:
            t.operations = self.operations

        fts = executor.map(run_task, self.tasks)
        logger.info("Running stage %d... (%s->%s): %d tasks" % (self.stage_id, self.surname_in, self.surname_out, self.num_tasks))
        res = executor.get_result(fts)
        # print(res)
        # print(sum(res))
        logger.info("Finished stage %d" % ( self.stage_id))

    def set_surname_in(self, surname: str):
        self.surname_in = surname

    def set_surname_out(self, surname: str):
        self.surname_out = surname


def gen_task_info(stage: Stage, task_id: int) -> TaskInfo:

    ti = TaskInfo(task_id = task_id,
                stage_id = stage.stage_id,
                job_id = stage.job_id,
                num_tasks = stage.num_tasks,
                read_path = stage.read_path,
                read_bucket = stage.read_bucket,
                write_path = stage.write_path,
                write_bucket = stage.write_bucket,
                surname_in = stage.surname_in,
                surname_out = stage.surname_out
    )

    ti.do_kp = stage.do_kp
    ti.partition = stage.partition

    return ti

def gen_data_info(stage: Stage) -> DataInfo:

    di = DataInfo(
        delimiter = stage.delimiter,
        types = stage.types,
        key =  stage.key if hasattr(stage, "key") else None
    )

    di.approx_rows = stage.approx_rows if hasattr(stage, "approx_rows") else None

    return di


