import pickle
from io import BytesIO
from typing import TextIO

import pandas as pd
from pandas import DataFrame


def serialize_to_file(df: DataFrame, f):
    pickle.dump(df, f)

def serialize(df: DataFrame) -> bytes:
    return pickle.dumps(df)

def deserialize(b: bytes) -> object:
    return pickle.loads(b)

# def serialize_to_file(df: DataFrame, f):
#     df.to_parquet(f, engine="pyarrow", compression="snappy")

# def deserialize(b: bytes) -> object:
#     return pd.read_parquet(BytesIO(bytes))
#
# def deserialize_from_file(f) -> object:
#     return pd.read_parquet(f)
