import pandas as pd

from seercloud.operation import Operation


class Sort(Operation):

    is_conservative = True
    key: str

    def __init__(self, key: str, **kwargs):

        super(Sort, self).__init__(**kwargs)
        self.key = key

    def sort(self, data: pd.DataFrame):

        data.sort_values(self.key, inplace=True)




