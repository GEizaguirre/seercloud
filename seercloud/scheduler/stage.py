import logging
from typing import List

from pandas import DataFrame

from seercloud.operation import Operation

logger = logging.getLogger(__name__)

class Stage():

    surname_in: str
    surname_out: str
    id: int
    operations: List[Operation]
    num_tasks: int

    def __init__(self, id, **kwargs):

        self.__dict__.update(**kwargs)
        self.operations = list()
        self.id = id


    def add_op(self, operation: Operation, **kwargs):
        self.operations.append(operation)

    def set_num_tasks(self, num_tasks: int):
        self.num_tasks = num_tasks


    def run(self):
        logger.info("Running stage %d... (%s->%s): %d tasks" % (self.id, self.surname_in, self.surname_out, self.num_tasks))
        pass

    def set_surname_in(self, surname: str):
        self.surname_in = surname

    def set_surname_out(self, surname: str):
        self.surname_out = surname






