import logging
from typing import Dict, List, Optional, Any, Tuple, Type

from seercloud.operation import Operation
from .stage import Stage

from lithops import FunctionExecutor

from seercloud.logging.logging import setup_logger
from seercloud.utils.scheduling import gen_surname
from ..IO.utils import get_data_size
from ..inference.infer import search_optimal

logger = logging.getLogger(__name__)


class Job():
    stages: Dict[int, Stage]
    dependencies: List[Tuple[int, int]]

    def __init__(self, num_stages: int = None, lithops_config: Optional[Dict[str, Any]] = None):

        self.executor = FunctionExecutor(config=lithops_config)
        self.storage = self.executor.storage
        self.lithops_config = lithops_config

        setup_logger()

        logger.info("Seer job created")

        if num_stages is None:
            self.stages = dict()
        else:
            self.stages = {s: Stage(s) for s in range(num_stages)}

        self.dependencies = list()

    def add(self, stage: int, op: Type[Operation], **kwargs):

        if stage not in self.stages.keys():
            self.stages[stage] = Stage()

        logger.info("Added operation %s to stage %d" % (op.__name__, stage))

        self.stages[stage].add_op(op(**kwargs))

    def run(self):

        self.prepare_execution()

        completed_stages = []
        stage_epochs = 0
        run_stage = True

        while len(completed_stages) < len(self.stages.keys()) and stage_epochs < len(self.stages.keys()):

            for s in self.stages.keys():
                if s not in completed_stages:
                    # Only run stages with completed dependencies
                    for d in self.dependencies:
                        if d[1] == s and d[0] not in completed_stages:
                            run_stage = False
                            break

                    if run_stage:
                        self.stages[s].run()
                        completed_stages.append(s)

            # At least 1 stage per epoch
            stage_epochs += 1

    def dependency(self, parent: int, child: int):

        if parent not in self.stages.keys() or child not in self.stages.keys():
            raise Exception("Incorrect dependency: stages ids are not present in the job.")

        self.dependencies.append((parent, child))
        logger.info("Added dependency %d->%d" % (parent, child))

    def prepare_execution(self):

        # Connect stage IOs
        self.connect_stages()

        # Define task and data info per stage based on operations
        self.prepare_parameters()

    def connect_stages(self):

        # TODO: Very rudimentary, only linear

        # Detect initial and end stages
        self.initial_stages = set()
        self.end_stages = set()

        for d in self.dependencies:
            if d[0] not in [dd[1] for dd in self.dependencies]:
                self.initial_stages.add(d[0])
            if d[1] not in [dd[0] for dd in self.dependencies]:
                self.end_stages.add(d[1])

        for d in self.initial_stages:
            self.stages[d].set_surname_in(gen_surname())
            self.stages[d].set_surname_out(gen_surname())

        for d in self.end_stages:
            self.stages[d].set_surname_out(gen_surname())

        for d in self.dependencies:
            self.stages[d[1]].surname_in = self.stages[d[0]].surname_out


        # Connect exchanges

    def prepare_parameters(self):

        #  Data info and task info based on first source operation (currently
        #  does not support multiple sources).
        for s in self.initial_stages:

            bucket = self.stages[s].operations[0].read_bucket
            path = self.stages[s].operations[0].read_path

            data_size = get_data_size(self.storage, bucket, path)

            logger.info("Stage %s: %.2f MB of data" % (s, data_size / (1024) ** 2))

            # Todo data types, aprox row size...

            worker_num = search_optimal(data_size, self.lithops_config)
            self.stages[s].num_tasks = worker_num

            current_s = s
            for d in self.dependencies:
                for d in self.dependencies:
                    if d[0] == current_s:
                        self.stages[d[1]].num_tasks = worker_num
                if d[0] == s:
                    current_s = d[1]

        # In exchange steps, type of operation

