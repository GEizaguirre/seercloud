from seercloud.IO.write import write_obj
from seercloud.operation import Operation
import pandas as pd
import pickle

from seercloud.scheduler.data import Data


class Write(Operation):

    write_count:int

    def __init__(self, **kwargs):

        super(Write, self).__init__(**kwargs)

        self.write_count = 0


    def run(self, data: Data):

        self.storage = self.data_info.storage

        if isinstance(data.data, pd.DataFrame):
            body = data.data.to_parquet(path=None, engine="pyarrow", compression="snappy")
        else:
            body = pickle.dumps(data.data)

        write_obj(storage=self.storage,
                  Bucket=self.task_info.write_bucket,
                  Key=self.task_info.read_path,
                  sufixes=["/out/%s/%d/%d"%(self.task_info.surname_out, self.write_count, self.task_info.task_id)],
                  Body= body)

        self.write_count += 1

