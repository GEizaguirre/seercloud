
from . import Operation
from ..scheduler.data import Data


class Collect(Operation):

    is_conservative = True

    def __init__(self, **kwargs):

       super(Collect, self).__init__(**kwargs)

    def run(self, data: Data):

        data.return_value = data.data
