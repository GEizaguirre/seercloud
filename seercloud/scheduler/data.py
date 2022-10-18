from typing import Union

import pandas as pd
import numpy as np

class Data:

    data: Union[pd.DataFrame, np.ndarray, int, float, str]
    hash_list: Union[list, np.ndarray]
    return_value: Union[pd.DataFrame, np.ndarray, int, float, str]
    read_bytes: int

    def __init__(self):
        self.data = None
        self.hash_list = None
        self.return_value = None
        self.read_bytes = 0

    def set_data(self, data: Union[pd.DataFrame, np.ndarray, int, float, str]):
        self.data = data

    def set_hash_list(self, hash_list: Union[list, np.ndarray]):
        self.hash_list = hash_list

    def set_return_value(self, return_value: Union[pd.DataFrame, np.ndarray, int, float, str]):
        self.return_value = return_value