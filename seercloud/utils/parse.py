from math import floor

from lithops import Storage

from seercloud.IO.read import read_and_adjust
from seercloud.IO.utils import get_data_size
from seercloud.metadata import DataInfo

READ_BLOCK = 50 * 1024
START_BYTES = 10 * 1024

def parse(storage: Storage, bucket: str, key: str, data_info: DataInfo):
    '''
    Approximate number of rows in dataset
    @param storage: storage client
    @param bucket: bucket to read
    @param key: path of the structured object to read
    @return: approximate number of rows of the object
    '''

    data_size = get_data_size(storage, bucket, key)

    df, part_size = read_and_adjust(storage=storage,
                                    read_bucket=bucket, read_path=key,
                                    data_info=data_info, lower_bound=START_BYTES,
                                    upper_bound=START_BYTES+READ_BLOCK, total_size=data_size)

    print(df.dtypes)

    row_size = floor(part_size / len(df))
    approx_rows = floor(data_size / row_size)

    types = [str(t) for t in df.dtypes]
    if "object" in types:
        types = None

    return {
        "approx_rows": approx_rows,
        "types": types
    }
