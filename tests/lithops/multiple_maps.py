import time

from lithops import FunctionExecutor

from tests.config import cloud_config

exec = FunctionExecutor(config=cloud_config())

def an(x):
    return str("a%d"%(x))

def bn(x):
    time.sleep(4)
    return str("b%d"%(x))

fts1 = exec.map(an, range(4))
fts2 = exec.map(bn, range(4))
assert(exec.get_result(fts2) == [ "b0", "b1", "b2", "b3"])
assert(exec.get_result(fts1) == [ "a0", "a1", "a2", "a3"])

# TODO: simplify asynchronous call based on this experiment
