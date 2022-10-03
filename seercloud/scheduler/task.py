from typing import Union, List

import numpy as np
import pandas as pd
from lithops import Storage

from seercloud.operation import Write, Exchange, Scan, Collect, Operation, Sort
from seercloud.operation.transform import Transformation
from seercloud.operation.groupby import Groupby
from seercloud.scheduler.data import Data

# TODO: log execution information

class Task():

    data: Data
    operations: List[Operation]
    storage: Storage

    def __init__(self):

        self.operations = list()
        self.current_operation = 0
        self.data = Data()


    def run(self):

        self.storage = Storage()

        for op in self.operations:
            # Share storage client in all operations
            op.data_info.set_storage(self.storage)
            op.run(Data)

        if self.data.return_value is not None:
            return self.data.return_value

    def add_operation(self, operation: Operation):
        self.operations.append(operation)




    def read(self):

        data, exchange_info = self.operation[0].scan()

        if isinstance(self.operations[1], Groupby) or isinstance(self.operations[1], Sort):
            self.current_operation += 1


    def transform(self):
        pass

    def write(self):
        pass

    def exchange(self):
        pass

    def collect(self):

        # append result to self.exec_info
        pass



def run_task(task: Task):

    task.run()



