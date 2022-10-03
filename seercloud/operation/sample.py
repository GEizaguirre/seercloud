import pickle
import random
import re

from lithops import Storage

from seercloud.IO.write import write_obj
from seercloud.IO.read import read_and_adjust
from seercloud.metadata import TaskInfo
import numpy as np

from seercloud.metadata.data_info import DataInfo

MAX_SAMPLE_SIZE:int = 300 * 1024 * 1024
SAMPLE_RATIO:float = 0.01
SAMPLE_FRAGMENTS:int = 20
START_MARGIN:float = 0.02
END_MARGIN:float = 0.02

SAMPLE_SUFIX:str = "sampled"

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

    def run(self):

        self.storage = Storage()

        ds_size = self.storage.head_object(self.read_bucket, self.read_path)['ContentLength']


        start_limit = int(ds_size * START_MARGIN)
        end_limit = int(ds_size * ( 1 - END_MARGIN ))

        fragment_size = min((end_limit-start_limit) * SAMPLE_RATIO, MAX_SAMPLE_SIZE) / SAMPLE_FRAGMENTS

        # read_bounds = np.linspace(start_limit, end_limit, SAMPLE_FRAGMENTS)
        # read_bounds = [ int(b) for b in read_bounds ]

        # Select bounds randomly
        num_parts = int(ds_size / fragment_size)
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
        if str(df['0'].dtype) not in ['bytes', 'str', 'object'] and \
                re.match("\|S[0-9]*", str(df['0'].dtype)) is None:
            segment_bounds = [ np.quantile(keys, q) for q in quantiles ]
        else:
            segment_bounds = [ keys[int(q * len(keys))] for q in quantiles]

        # write function (path, op_sufix, task)
        sufixes = [ SAMPLE_SUFIX, self.task_info.surname ]
        write_obj(self.storage, self.read_bucket, self.read_path, sufixes, pickle.dumps(segment_bounds))


def run_sample(sample: Sample):
    sample.run()