import logging
import pickle
from concurrent.futures.thread import ThreadPoolExecutor
from enum import Enum
from math import floor
from typing import Tuple, Union
from queue import Queue
import numpy as np
from lithops.storage import Storage
import os
import pandas as pd

from seercloud.IO.read import read_and_adjust, adjust_bounds, read_obj
from seercloud.IO.utils import get_data_size
from seercloud.inference.config import CHUNK_SIZE
from seercloud.operation import Operation
from seercloud.config import DEFAULT_THREAD_NUM
from seercloud.operation.Partition import Partition
from seercloud.operation.sample import SAMPLE_SUFIX
from seercloud.scheduler.data import Data
from seercloud.utils.hash import hash2


DATAFRAME_ALLOCATION_MARGIN = 0.1

logger = logging.getLogger(__name__)


class Scan(Operation):

    is_conservative = True
    is_source = True

    task_id: int
    num_tasks: int
    read_path: str
    read_bucket: str
    storage: Storage

    q_writes: Queue
    q_reads: Queue
    q_finish: Queue

    read_bounds: list
    num_bounds: int

    delimiter: str

    def __init__(self, file: str, bucket: str, types: list = None, delimiter: str = ",", **kwargs):

        super(Scan, self).__init__(**kwargs)

        self.read_path = file
        self.read_bucket = bucket
        self.chunk_size = CHUNK_SIZE
        self.types = types
        self.delimiter = delimiter

    def run(self, data: Data):

        self.storage = self.data_info.storage

        if self.task_info.partition == Partition.SEGMENT:
            self.get_segment_info()

        # Calculate read range
        self._get_read_range()

        self.read_bounds = list(range(self.lower_bound, self.upper_bound, self.chunk_size))
        self.num_bounds = len(self.read_bounds)

        self.read_bounds.append(self.upper_bound)
        # for i in range(self.num_bounds - 1):
        #     lb, ub = self._adjust_bounds(self.read_bounds[i], self.read_bounds[i + 1])
        #     self.read_bounds[i] = lb

        # key-pointer conditions
        if self.task_info.do_kp:
            print("Concurrent scan and partitioning %s"%("with hash" if self.task_info.do_hash else "None"))
            ed, hash_list = self._concurrent_read(self.task_info.partition, self.key)
            data.data = ed
            data.hash_list = hash_list
        else:
            print("Simple scan")
            self._chunk_merger(data)



    def _concurrent_read(self, do_hash: bool, key: str) -> Tuple[pd.DataFrame, Union[list, np.ndarray, None]]:

        self.q_writes = Queue(self.num_bounds - 1)
        self.q_reads = Queue(self.num_bounds - 1)
        self.q_finish = Queue()

        for i in range(self.num_bounds - 1):
            self.q_reads.put_nowait(i)

        with ThreadPoolExecutor(max_workers=DEFAULT_THREAD_NUM) as pool:
            consumer_future = pool.submit(self._chunk_consumer_kp, do_hash, key)
            pool.submit(self._chunk_producer, 0)

        ed, hash_list = consumer_future.result()

        return ed, hash_list

    def _chunk_producer(self):

        try:
            while not self.q_reads.empty():

                if self.q_finish.empty() is not True:
                    return None

                c_i = self.q_reads.get()

                # read_part = self.storage.get_object(self.read_bucket, self.read_path,
                #                                     extra_get_args={"Range": ''.join(
                #                                         ['bytes=', str(self.read_bounds[c_i]), '-',
                #                                          str(self.read_bounds[c_i + 1] - 1)])
                #                                     })
                #
                # df = pd.read_csv(read_part, engine='c', index_col=None, header=None,
                #                  names=self.data_info.columns,
                #                  delimiter=self.data_info.delimiter, dtype=self.data_info.dtypes)

                # part_size = len(read_part)

                df, part_size = read_and_adjust(storage=self.storage, read_bucket=self.read_bucket,
                                                read_path=self.read_path, data_info = self.data_info,
                                                lower_bound = self.read_bounds[c_i], upper_bound= self.read_bounds[c_i + 1] - 1,
                                                total_size=self.total_size)


                with open("c_{}".format(c_i), 'wb') as f:
                    pickle.dump(df, f)

                self.q_writes.put(["c_{}".format(c_i), part_size])

            self.q_writes.put([-1, -1])

        except Exception as error:
            print("Producer exception: {}".format(str(error)))

    def _chunk_consumer_kp(self, partition: Partition, key: str) -> Tuple[pd.DataFrame, list]:

        read_cnks = 0
        current_lower_bound = 0
        accumulated_size = 0

        # Key-pointer structure
        my_row_num = int((self.data_info.approx_rows / self.task_info.num_tasks) * (1 + DATAFRAME_ALLOCATION_MARGIN))

        df_columns = {nm: np.empty(my_row_num,
                                   dtype=self.data_info.dtypes[nm]) for nm in self.data_info.columns}
        key_pointer_struct = {
            'key': np.empty( my_row_num,  dtype= 'uint32' )
        }

        while True:

            # Check if task has been signaled to end

            try:
                [chunk_name, chunk_size] = self.q_writes.get(block=True, timeout=30)
            except Exception as e:
                logging.info("Writer: Timeout occurred {}".format(str(e)))
                return None, None

            if chunk_name == -1:
                break
            accumulated_size += chunk_size

            # Read intermediate file
            with open(chunk_name, 'rb') as f:
                df = pickle.load(f)

            current_shape = len(df)

            os.remove(chunk_name)

            keys_array = np.array(df[key])

            if partition.HASH:
                keys_array = np.array(hash2(keys_array, self.task_info.num_tasks), dtype='uint32')
            if partition.SEGMENT:
                keys_array = np.searchsorted(self.segment_info, keys_array)

            key_pointer_struct['key'][current_lower_bound:(current_lower_bound + current_shape)] = keys_array

            del keys_array

            for nm in self.data_info.columns:
                df_columns[nm][current_lower_bound:(current_lower_bound + current_shape)] = df[nm]

            current_lower_bound = current_lower_bound + current_shape

            read_cnks += 1

            # if speculative and not is_speculative:
            #     _upload_progress(ibm_cos, range_i, accumulated_size / total_bytes, time.time() - start_time, metadata)

            # Check if task has been signaled to end
            # if speculative:
            #     if _check_end_signal(ibm_cos, metadata, fname):
            #         q_finish.put(1)
            #         return None, None

        del df

        for nm in self.data_info.columns:
            df_columns[nm] = df_columns[nm][0:current_lower_bound]

        hash_list = key_pointer_struct['key'][0:current_lower_bound]

        del key_pointer_struct['key']

        for nm in self.data_info.columns:
            df_columns[nm] = df_columns[nm][0:current_lower_bound]

        df = pd.DataFrame(df_columns)

        return df, hash_list

    def _chunk_merger(self, data: Data) -> Tuple[pd.DataFrame, list]:
        # only reads and finally merges

        chunks = []

        print(self.read_bounds)

        for c_i in range(self.num_bounds):


            chunk, part_size = read_and_adjust(storage=self.storage, read_bucket=self.read_bucket,
                                               read_path=self.read_path, data_info=self.data_info,
                                               lower_bound=self.read_bounds[c_i],
                                               upper_bound=self.read_bounds[c_i + 1] - 1,
                                               total_size=self.total_size)

            # print(part_size)

            data.read_bytes += part_size

            chunks.append(chunk)

            print(len(chunk))

        df = pd.concat(chunks, ignore_index=True)

        # print(len(df))

        data.data = df

        print(self.task_info.partition)
        if self.task_info.partition == Partition.HASH:
            data.hash_list = np.array(hash2(data.data[self.key], self.task_info.num_tasks), dtype='uint32')
        if self.task_info.partition == Partition.SEGMENT:

            print(self.segment_info)
            data.hash_list = np.searchsorted(self.segment_info, data.data[self.key])
            print(len)

        print(data.hash_list[:10])



    def _get_read_range(self):
        """
        Calculate byte range to read from a dataset, given the id of the task.
        """

        self.total_size = get_data_size(self.storage, self.read_bucket, self.read_path)

        partition_size = floor(self.total_size / self.task_info.num_tasks)

        self.lower_bound = self.task_info.task_id * partition_size
        self.upper_bound = self.lower_bound + partition_size

        # self.lower_bound, self.upper_bound = adjust_bounds(self.storage, self.read_bucket, self.read_path,
        #                                                    self.lower_bound, self.upper_bound, self.total_size)

        print("Scanning bytes=%d-%d (%d)"%(self.lower_bound, self.upper_bound,
                                           self.upper_bound - self.lower_bound))


    def explain(self):
        return "%s (%s/%s)"%(self.__class__.__name__, self.read_bucket, self.read_path)

    def get_segment_info(self):

        sufixes = [SAMPLE_SUFIX, self.task_info.surname_out]

        try:

            self.segment_info = pickle.loads(read_obj(self.storage, self.read_bucket,
                                         self.read_path, sufixes))

        except Exception as e:

            self.segment_info = None

