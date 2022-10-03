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

    def set_task_info(self, **kwargs):
        self.task_info = TaskInfo(**kwargs)

    def set_data_info(self, **kwargs):
        self.data_info = DataInfo(**kwargs)
