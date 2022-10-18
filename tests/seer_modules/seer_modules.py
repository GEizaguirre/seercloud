from seercloud.utils.scheduling import gen_surname
from tests.config import cloud_config
from lithops import FunctionExecutor

exec = FunctionExecutor(config = cloud_config())

def foo(foo_data):
    return gen_surname()

fts = exec.call_async(foo, 0)
print(exec.get_result(fts))

