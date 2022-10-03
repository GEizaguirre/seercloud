import asyncio
import functools
import http
import pickle
import random
import time
from typing import Union

from ibm_botocore.exceptions import ClientError
from lithops import Storage

from seercloud.IO.read import read_obj
from seercloud.IO.write import write_obj
from seercloud.operation import Operation
import pandas as pd
import numpy as np

from seercloud.scheduler.data import Data

PART_NUM_SUFIX:str = "meta"
MAX_RETRIES:int = 10
MAX_READ_TIME:int = 50
RETRY_WAIT_TIME:float = 1

class Exchange(Operation):

    write: bool
    storage: Storage

    def __init__(self, write: bool, **kwargs ):

        super(Exchange, self).__init__(**kwargs)
        self.write = write


    def run(self, data: Data):

        self.storage = self.data_info.storage

        # TODO: Define direct/2level

        if self.write:
            self.write_direct(data)
        else:
            self.read_direct(data)



    def write_direct(self, data: Data):

        ed = data.data
        hash_list = data.hash_list

        parts = [ ]
        upload_info = { 'row_num': {} }

        for w in self.task_info.num_tasks:

            # Get rows corresponding to this worker
            pointers_ni = np.where(hash_list == w)[0]

            # print(ni)
            pointers_ni = np.sort(pointers_ni.astype("uint32"))

            lower_bound = 0

            fname = "%d.pyarrow"%(w)

            with open(fname, "wb") as f:
                ed.iloc[pointers_ni].to_parquet(f, engine="pyarrow", compression="snappy")

            parts.append((w, fname))

            upload_info['row_num'][w] = len(pointers_ni)

        # randomly shuffle bounds (better throughput distribution)
        random.shuffle(parts)

        async def _writes(part_info):

            loop = asyncio.get_event_loop()

            objects = await asyncio.gather(
                *[
                    loop.run_in_executor(None, functools.partial(write_obj,
                                                                 storage = self.storage,
                                                                 Bucket = self.task_info.write_bucket,
                                                                 Key = self.task_info.read_path,
                                                                 sufixes = [self.surname_out,
                                                                            str(self.task_info.task_id),
                                                                            b[0]],
                                                                 # Body=b[1]))
                                                                 Body=open(b[1], "rb")))
                    for b in part_info
                ]
            )
            return objects

        loop = asyncio.get_event_loop()
        _ = loop.run_until_complete(_writes(parts))

        write_obj(storage = self.storage,
                 Bucket = self.task_info.write_bucket,
                 Key = self.task_info.read_path,
                 sufixes = [self.surname_out,
                            str(self.task_info.task_id),
                            PART_NUM_SUFIX],
                 # Body=b[1]))
                 Body=pickle.dumps(upload_info))

    def read_direct(self) -> pd.DataFrame:



        def _reader(pi):

            retry = 0
            before_readt = time.time()

            while retry < MAX_RETRIES:

                try:

                    data = read_obj(
                        storage = self.storage,
                        Bucket = self.task_info.write_bucket,
                        Key = self.task_info.read_path,
                        sufixes = [self.surname_out,
                                   str(pi),
                                   self.task_info.task_id])

                    return { "data": data }


                except ClientError as ex:
                    if ex.response['Error']['Code'] == 'NoSuchKey':
                        if time.time() - before_readt > MAX_READ_TIME:
                            return None
                    time.sleep(RETRY_WAIT_TIME)
                    continue

                except (http.client.IncompleteRead) as e:
                    if retry == MAX_RETRIES:
                        return None
                    retry += 1
                    continue

                except Exception as e:
                    return None

        async def reads():

            parts = list(range(self.task_info.num_tasks))
            random.shuffle(parts)

            tasks = [
                loop.run_in_executor(None, functools.partial(_reader, ps=ll))
                for ll in parts
            ]

            objects = await asyncio.gather(
                *tasks
            )

            return objects

        loop = asyncio.get_event_loop()
        res = loop.run_until_complete(reads())

        res = [ r for r in res if r is not None ]

        ed = pd.concat([ pd.read_parquet(r["data"]) for r in res ])

        return ed


    def write_2level(self):

        pass

    def read_2level(self):

        pass

def infere(self):

    # Infere exchange method and number of workers

    pass


def infere_method(self):

    pass

def infere_workers(self):

    pass

