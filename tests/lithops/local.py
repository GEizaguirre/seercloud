from lithops import FunctionExecutor
from lithops.storage import Storage

from tests.config import local_config

exec = FunctionExecutor(config = local_config())

# Basic test
def incr(x):
    return x+1

fts = exec.map(incr, range(5))
assert(exec.get_result(fts) == [ i+1 for i in range(5)] )


# IO test
def write(x):
    storage = Storage()

    storage.put_object(bucket="sandbox", key="%d"%(x), body=str(x+5))


def read(x):
    storage = Storage()

    res = int(storage.get_object(bucket="sandbox", key="%d"%(x)))

    return res


fts = exec.map(write, range(5))
exec.wait(fts)

fts = exec.map(read, range(5))
assert(exec.get_result(fts) == [ i+5 for i in range(5)] )

def get_head(path:str):
    storage = Storage()
    return storage.head_object(bucket="sandbox", key=path)

fts = exec.call_async(get_head, "terasort_1GB.csv")
print(exec.get_result(fts))


def get_part(path:str):
    storage = Storage()
    return storage.get_object(bucket="sandbox", key=path, extra_get_args={ "Range": "bytes=0-100"} )

fts = exec.call_async(get_part, "terasort_1GB.csv")
print(exec.get_result(fts))