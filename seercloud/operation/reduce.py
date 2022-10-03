from typing import Callable

from . import Operation
from ..scheduler.data import Data


class Reduce(Operation):

    is_conservative = False

    def __init__(self, action: Callable, **kwargs):

        super(Reduce, self).__init__(**kwargs)
        self.action = action

    def run(self, data: Data):
        data.data = self.action(data.data)
