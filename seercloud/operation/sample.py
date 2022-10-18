import logging
import pickle
import random
import re
from math import floor

from lithops import Storage, FunctionExecutor

from seercloud.IO.utils import get_data_size
from seercloud.IO.write import write_obj
from seercloud.IO.read import read_and_adjust
from seercloud.metadata import TaskInfo
import numpy as np

from seercloud.metadata.data_info import DataInfo
from seercloud.scheduler.stage import Stage, gen_data_info, gen_task_info

MAX_SAMPLE_SIZE:int = 300 * 1024 * 1024
SAMPLE_RATIO:float = 0.01
SAMPLE_FRAGMENTS:int = 20
START_MARGIN:float = 0.02
END_MARGIN:float = 0.02

SAMPLE_SUFIX:str = "sampled"

logger = logging.getLogger(__name__)

# TODO: sampling of intermediate data

class Sample():

    storage: Storage

    data_info: DataInfo
    task_info: TaskInfo

    read_bucket: str
    read_path: str


    def __init__(self, read_bucket: str, read_path: str,
                 data_info: DataInfo, task_info: TaskInfo, key: str):

        self.read_bucket = read_bucket
        self.read_path = read_path
        self.data_info = data_info
        self.task_info = task_info
        self.key = key

    def _run(self):

        self.storage = Storage()


        ds_size = get_data_size(self.storage, self.read_bucket, self.read_path)


        start_limit = int(ds_size * START_MARGIN)
        end_limit = int(ds_size * ( 1 - END_MARGIN ))

        choosable_size = end_limit - start_limit

        fragment_size = floor(min(floor((end_limit-start_limit) * SAMPLE_RATIO), MAX_SAMPLE_SIZE) / SAMPLE_FRAGMENTS)

        # read_bounds = np.linspace(start_limit, end_limit, SAMPLE_FRAGMENTS)
        # read_bounds = [ int(b) for b in read_bounds ]

        # Select bounds randomly
        num_parts = int(choosable_size / fragment_size)
        selected_fragments = sorted(random.sample(range(num_parts), SAMPLE_FRAGMENTS))

        keys_arrays = []


        # Read from each bound a fragment size, adjusting limits
        for f in selected_fragments:

            lower_bound = start_limit + f * fragment_size
            upper_bound = lower_bound + fragment_size

            df, part_size = read_and_adjust(storage = self.storage,
                                            read_bucket= self.read_bucket, read_path=self.read_path,
                                            data_info = self.data_info, lower_bound = lower_bound,
                                            upper_bound=upper_bound, total_size=end_limit)

            keys_arrays.append(np.array(df[self.key]))

        # Concat keys, sort them
        keys = np.concatenate(keys_arrays)
        keys.sort()

        # Find quantiles (num tasks)
        quantiles = [ i * 1 / self.task_info.num_tasks for i in range(1, self.task_info.num_tasks) ]
        if str(df[self.key].dtype) not in ['bytes', 'str', 'object'] and \
                re.match("\|S[0-9]*", str(df[self.key].dtype)) is None:
            segment_bounds = [ np.quantile(keys, q) for q in quantiles ]
        else:
            segment_bounds = [ keys[int(q * len(keys))] for q in quantiles]

        # write function (path, op_sufix, task)
        sufixes = [ SAMPLE_SUFIX, self.task_info.surname_out ]
        write_obj(self.storage, self.read_bucket, self.read_path, sufixes, pickle.dumps(segment_bounds))


    def run(self, executor: FunctionExecutor):

        logger.info("Running Sample %d... (%s)" % (self.task_info.stage_id, self.task_info.surname_out))

        ft = executor.call_async(run_sample, self)
        print(executor.get_result(ft))

    def explain(self):

        return "%s (%s/%s)" % (self.__class__.__name__, self.read_bucket, self.read_path)


def gen_sample_stage(stage: Stage):

    data_info: DataInfo = gen_data_info(stage)
    task_info: TaskInfo = gen_task_info(stage, 0)

    return Sample(read_bucket = task_info.read_bucket,
                  read_path = task_info.read_path,
                  data_info = data_info,
                  task_info = task_info,
                  key = data_info.key )

def run_sample(sample: Sample):

    return sample._run()