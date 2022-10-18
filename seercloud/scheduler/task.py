from typing import Union, List

from lithops import Storage

import logging
from seercloud.metadata import DataInfo, TaskInfo
from seercloud.operation import Operation
from seercloud.scheduler.data import Data


# TODO: save execution metrics

logger = logging.getLogger(__name__)

class Task():

    data_info: DataInfo
    task_info: TaskInfo
    operations: List[Operation]
    storage: Storage

    def __init__(self, task_info: TaskInfo, data_info: DataInfo):

        self.operations = list()
        self.current_operation = 0
        self.data = Data()

        self.task_info = task_info
        self.data_info = data_info


    def run(self):

        self.storage = Storage()

        for op in self.operations:

            print("Running %s"%(op.__class__.__name__))

            op.set_task_info(self.task_info)
            op.set_data_info(self.data_info)

            # Share storage client in all operations
            op.data_info.set_storage(self.storage)
            op.run(self.data)

        if self.data.return_value is not None:
            return self.data.return_value
        else:
            return len(self.data.data)

    def set_operations(self, operations: List[Operation]):
        self.operations = operations


def run_task(task: Task):

    print("Stage %d - task %d" % (task.task_info.stage_id, task.task_info.task_id))

    return task.run()



