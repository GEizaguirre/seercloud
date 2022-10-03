from operator import xor
from struct import unpack

from seercloud.src.chash import chash
import numpy as np
import hashlib

# def stable_hash(a_string, part_num: int):
#     sha256 = hashlib.sha256()
#     sha256.update(bytes(a_string, "UTF-8"))
#     digest = sha256.digest()
#     h = 0
#     #
#     for index in range(0, len(digest) >> 3):
#         index8 = index << 3
#         bytes8 = digest[index8 : index8 + 8]
#         i = unpack('q', bytes8)[0]
#         h = xor(h, i)
#     #
#     return h % part_num

def stable_hash(string: str, part_num: int):
    return  int(hashlib.md5(string.encode("UTF-8")).hexdigest(), 32) % part_num

def hash2(key_array: np.ndarray, part_number: int):

    type = str(key_array.dtype)

    if type.startswith("int") or type.startswith("float"):
        return chash(key_array, len(key_array), part_number)
    else:
        return np.array([ stable_hash(s, part_number) for s in key_array ])




