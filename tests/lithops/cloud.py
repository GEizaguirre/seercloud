from lithops import FunctionExecutor
from lithops.storage import Storage
import yaml

from tests.config import cloud_config

exec = FunctionExecutor(config = cloud_config())

# Basic test
def incr(x):
    return x+1

fts = exec.map(incr, range(5))
assert(exec.get_result(fts) == [ i+1 for i in range(5)] )


# IO test
def write(x):
    storage = Storage()

    storage.put_object(bucket="seer-data", key="test/%d"%(x), body=str(x+5))


def read(x):
    storage = Storage()

    res = int(storage.get_object(bucket="seer-data", key="test/%d"%(x)))

    return res


fts = exec.map(write, range(5))
exec.wait(fts)

fts = exec.map(read, range(5))
assert(exec.get_result(fts) == [ i+5 for i in range(5)] )