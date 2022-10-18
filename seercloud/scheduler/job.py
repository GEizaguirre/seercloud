import logging
import random
from typing import Dict, List, Optional, Any, Tuple, Type

from seercloud.operation import Operation, Exchange, Sort, Scan
from .stage import Stage, gen_data_info

from lithops import FunctionExecutor, Storage

from seercloud.logging.logging import setup_logger
from seercloud.utils.scheduling import gen_surname
from ..IO.utils import get_data_size
from ..inference.infer import search_optimal
from ..operation.groupby import Groupby
from ..operation.sample import gen_sample_stage
from ..utils.key_pointer import _partition_conds, _key_pointer_conds
from ..utils.parse import parse

logger = logging.getLogger(__name__)


class Job():

    stages: Dict[int, Stage]
    dependencies: List[Tuple[int, int]]
    storage: Storage

    def __init__(self, num_stages: int = None, lithops_config: Optional[Dict[str, Any]] = None):

        self.executor = FunctionExecutor(config=lithops_config)
        self.storage = self.executor.storage
        self.lithops_config = lithops_config
        self.job_id = random.randint(0, 10000)

        setup_logger()

        logger.info("Seer job created")

        if num_stages is None:
            self.stages = dict()
        else:
            self.stages = {s: Stage(s, self.job_id) for s in range(num_stages)}

        self.dependencies = list()
        self.preparatory_steps = dict()

    def add(self, stage: int, op: Type[Operation], **kwargs):

        if stage not in self.stages.keys():
            self.stages[stage] = Stage(stage, self.job_id)

        logger.info("Added operation %s to stage %d" % (op.__name__, stage))

        if op is Exchange:
            self.stages[stage].add_op(op(**kwargs, write=True))
        else:
            self.stages[stage].add_op(op(**kwargs))

    def run(self):

        self.prepare_execution()

        self.set_preparatory_steps()

        self.explain()

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

                        for prep in self.preparatory_steps[s]:
                            prep.run(self.executor)
                        self.stages[s].run(self.executor)
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
            # if d[1] not in [dd[0] for dd in self.dependencies]:
            #     self.end_stages.add(d[1])

        for s in self.initial_stages:
            self._setup_initial(self.stages[s])

        #
        #
        # for d in self.end_stages:
        #     self.stages[d].set_surname_out(gen_surname())

        for d in self.dependencies:
            self._connect_stages(d[1], d[0])


    def _setup_initial(self, stage: Stage):

        stage.set_surname_in(gen_surname())
        stage.set_surname_out(gen_surname())

        source_operation: Scan = stage.operations[0]

        stage.read_path = source_operation.read_path
        stage.read_bucket = source_operation.read_bucket
        stage.write_bucket = stage.read_bucket
        stage.write_path = stage.read_path
        stage.delimiter = source_operation.delimiter
        stage.types = source_operation.types

        parsed_data = parse(storage = self.storage,
                                  bucket=source_operation.read_bucket,
                                  key=source_operation.read_path,
                                  data_info=gen_data_info(stage))

        stage.approx_rows = parsed_data["approx_rows"]
        if parsed_data["types"] is not None:
            stage.types = parsed_data["types"]

        logger.info("%s/%s: approximately %d records"%(source_operation.read_bucket,
                                                        source_operation.read_path,
                                                        stage.approx_rows))
        logger.info("Data types: %s"%(str(stage.types)))



    def _connect_stages(self, child_stage_id: int, parent_stage_id: int):

        child = self.stages[child_stage_id]
        parent = self.stages[parent_stage_id]

        child.surname_in = parent.surname_out
        child.set_surname_out(gen_surname())
        if isinstance(parent.operations[-1], Exchange):
            child.operations.insert(0, Exchange(write=False))

        if True in [ isinstance(op, Sort) or isinstance(op, Groupby) for op in child.operations ]:
            for op in child.operations:
                if isinstance(op, Sort) or isinstance(op, Groupby):
                    shuffle_op = op
                    break
            child.key = shuffle_op.key
            parent.key = shuffle_op.key

        child.read_path = parent.read_path
        child.read_bucket = parent.read_bucket
        child.write_bucket = parent.write_bucket
        child.write_path = parent.write_path
        child.delimiter = parent.delimiter
        child.types = parent.types
        child.approx_rows = parent.approx_rows



    def prepare_parameters(self):

        #  Data info and task info based on first source operation (currently
        #  does not support multiple sources).
        for s in self.initial_stages:

            bucket = self.stages[s].operations[0].read_bucket
            path = self.stages[s].operations[0].read_path

            data_size = get_data_size(self.storage, bucket, path)

            logger.info("Stage %s: %.2f MB of data" % (s, data_size / (1024) ** 2))


            worker_num = search_optimal(data_size, self.lithops_config)
            self.stages[s].num_tasks = worker_num

            current_s = s
            for d in self.dependencies:
                for d in self.dependencies:
                    if d[0] == current_s:
                        self.stages[d[1]].num_tasks = worker_num
                if d[0] == s:
                    current_s = d[1]

        for s_id, stage in self.stages.items():

            _partition_conds(self.stages, self.dependencies, s_id)

            _key_pointer_conds(self.stages, self.dependencies, s_id)

    def set_preparatory_steps(self):

        self.preparatory_steps = { si: list() for si in self.stages.keys() }

        # Detect prepararatory steps (sampling, parsing...)
        for d in self.dependencies:
            if True in [ isinstance(op, Sort) for op in self.stages[d[1]].operations ]:
                if isinstance(self.stages[d[1]].operations[0], Exchange):
                    self.preparatory_steps[0].append(
                        gen_sample_stage(self.stages[d[0]])
                    )

    def explain(self):

        for si, s in self.stages.items():
            for op in self.preparatory_steps[si]:
                print("prep: %s"%(op.explain()))
            print("Stage %d:"%(si))
            for op in s.operations:
                print("\t· %s"%(op.explain()))


