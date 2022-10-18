from seercloud.metadata import TaskInfo
from seercloud.metadata.data_info import DataInfo
from seercloud.scheduler.data import Data


class Operation:

    is_conservative: bool
    is_source: bool

    task_info: TaskInfo
    data_info: DataInfo

    def __init__(self):
        pass

    def run(self, data: Data):
        pass

    def set_task_info(self, task_info: TaskInfo):
        self.task_info = task_info

    def set_data_info(self, data_info: DataInfo ):
        self.data_info = data_info

    def explain(self) -> str:
        return self.__class__.__name__
