import pandas as pd

from seercloud.operation import Operation
from seercloud.scheduler.data import Data


class Sort(Operation):

    is_conservative = True
    key: str

    def __init__(self, key: str, **kwargs):

        super(Sort, self).__init__(**kwargs)
        self.key = key

    def run(self, data: Data):

        data.data.sort_values(self.key, inplace=True)




