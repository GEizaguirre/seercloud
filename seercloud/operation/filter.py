from typing import Callable

from seercloud.operation import Operation
from seercloud.scheduler.data import Data


class Filter(Operation):

    is_conservative = False

    def __init__(self, action: Callable, **kwargs):

        super(Filter, self).__init__(**kwargs)
        self.action = action

    def run(self, data: Data):
        data.data = self.action(data.data)