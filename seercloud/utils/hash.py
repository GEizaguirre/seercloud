from operator import xor
from struct import unpack

# TODO: correct hash importation on cloud
# from seercloud.src.chash import chash
import numpy as np
import hashlib

def stable_hash(obj, part_num: int):
    return  int(hashlib.md5(str(obj).encode("UTF-8")).hexdigest(), 32) % part_num

def hash2(key_array: np.ndarray, part_number: int):

    type = str(key_array.dtype)

    # if type.startswith("int") or type.startswith("float"):
    #     return chash(key_array, len(key_array), part_number)
    # else:
    #     return np.array([ stable_hash(s, part_number) for s in key_array ])
    return np.array([stable_hash(s, part_number) for s in key_array])




