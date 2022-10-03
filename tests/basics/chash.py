from seercloud.utils.hash import hash2
import numpy as np
import string
import random
import time

res = hash2(np.array([1343442,35534234,324433434], dtype="int64"), 10 )
print(res)
assert res ==  [ 2, 4, 4 ]
print("basic chash test passed")


res = hash2(np.array([455.244,23455.2454,234533.223444,24345.234]), 15)
print(res)


res = hash2(np.array(["gfhfdjhf", "fjdwbfjekfe", "jefhk"]), 5)

def randStr(chars = string.ascii_uppercase + string.digits, N=10):
	return ''.join(random.choice(chars) for _ in range(N))

L = 3145728
key_array = np.array([  randStr() for _ in range(L) ])

start = time.time()
res = hash2(key_array, 41)
end = time.time()
print(end - start)
