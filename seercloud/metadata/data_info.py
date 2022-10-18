import pandas as pd
import numpy as np
from lithops import Storage



class DataInfo():

    delimiter: str
    columns: list
    types: list
    chunk_size: int

    dtypes: dict
    approx_rows: int

    storage: Storage

    key: str

    def __init__(self, delimiter : str = ",", columns : list = None, types : list = None,
                 chunk_size: int = 64 * 1024 ** 2, **kwargs):

        self.delimiter = delimiter
        self.columns = columns
        self.types = types
        self.chunk_size = chunk_size

        self.__dict__.update(kwargs)

        self.mount_df()

    def set_storage(self, storage: Storage):
        self.storage = storage


    def mount_df(self):


        if self.types is not None and (self.columns) != len(self.types):
            raise Exception("Lengths of column and types must be the same.")

        if self.types is not None:
            if self.columns is None:
                # self.columns = ["".join(["c", str(r)]) for r in range(len(self.types))]
                self.columns = [ str(r) for r in range(len(self.types))]
            self.normalize_types()
            self.dtypes = { c:t for c, t in zip(self.columns, self.types) }
        else:
            self.dtypes = None


    def normalize_types(self):
        self.types = [ np.dtype(n).name for n in self.types ]

    def set_key(self, key:str):
        self.key = key





