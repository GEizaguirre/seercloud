from typing import List, Union, TextIO, BinaryIO

from lithops import Storage


def write_obj(storage: Storage, Bucket: str, Key: str,
              sufixes: List[str], obj: Union[str, bytes, TextIO, BinaryIO]):
    storage.put_object(Bucket,
                       "_".join([Key] + sufixes),
                       obj)