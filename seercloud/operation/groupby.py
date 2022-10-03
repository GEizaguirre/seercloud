from seercloud.operation import Operation


class Groupby(Operation):

    key: str

    def __init__(self, key: str = '0'):

        self.key = key

