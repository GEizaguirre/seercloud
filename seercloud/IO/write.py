from typing import List, Union, TextIO, BinaryIO

from lithops import Storage


def write_obj(storage: Storage, Bucket: str, Key: str,
              sufixes: List[str], Body: Union[str, bytes, TextIO, BinaryIO],
              delimiter:str = "_"):
    storage.put_object(Bucket,
                       delimiter.join([Key] + sufixes),
                       Body)