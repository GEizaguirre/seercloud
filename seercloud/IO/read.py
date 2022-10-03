from io import BytesIO, StringIO
from typing import Tuple, Union, List

import pandas as pd
from lithops import Storage

from seercloud.metadata import DataInfo


BOUND_EXTRACTION_MARGIN = 100

def read_and_adjust(storage: Storage, read_bucket: str, read_path: str,
                    data_info: DataInfo, lower_bound: int, upper_bound: int,
                    total_size: int) -> Tuple[pd.DataFrame, int]:

    lower_bound, upper_bound = adjust_bounds(storage, read_bucket, read_path,
                                             lower_bound, upper_bound, total_size)


    read_part = storage.get_object(read_bucket, read_path,
                                        extra_get_args={"Range": ''.join(
                                            ['bytes=', str(lower_bound), '-',
                                             str(upper_bound)])
                                        })

    read_part = part_to_IO(read_part)


    df = pd.read_csv(read_part, engine='c', index_col=None, header=None,
                     names=data_info.columns,
                     delimiter=data_info.delimiter, dtype=data_info.dtypes)

    return df, len(read_part)


def adjust_bounds(storage: Storage, read_bucket: str, read_path: str,
                    lower_bound: int, upper_bound: int, total_size: int) -> Tuple[int, int]:
        """
        Adjust byte range to read complete lines of structured data.
        @param lower_bound: approximate byte to start the read.
        @param upper_bound: approximate byte to end the read.
        @return: specific byte range to read without cutting lines.
        """
        if lower_bound != 0:

            # lower_bound definition
            plb = lower_bound - BOUND_EXTRACTION_MARGIN
            pub = lower_bound + BOUND_EXTRACTION_MARGIN
            if plb < 0:
                plb = 0
            if pub > total_size:
                pub = total_size
            marked_pos = lower_bound - plb

            ### cos read of bytes=plb,pub
            chunk = storage.get_object(read_bucket,
                                            read_path,
                                            extra_get_args={"Range": ''.join(
                                                ['bytes=', str(plb), '-',
                                                 str(pub)])
                                            })

            while (plb != 0) and (b'\n' not in chunk[:marked_pos]):
                plb = plb - BOUND_EXTRACTION_MARGIN
                # cos read of bytes=plb,pub

                chunk = storage.get_object(read_bucket,
                                                read_path,
                                                extra_get_args={"Range": ''.join(
                                                    ['bytes=', str(plb), '-',
                                                     str(pub)])
                                                })

                marked_pos = lower_bound - plb

            if plb != 0:
                for i in reversed(range(0, marked_pos)):
                    if chunk[i:i + 1] == b'\n':
                        plb = plb + i + 1
                        break
            lb = plb

        ub = upper_bound
        if ub < total_size:

            # upper bound definition
            plb = upper_bound - BOUND_EXTRACTION_MARGIN
            pub = upper_bound + BOUND_EXTRACTION_MARGIN
            if plb < 0:
                plb = 0
            if pub > total_size:
                pub = total_size

            marked_pos = upper_bound - plb

            if pub != total_size:

                chunk = storage.get_object(read_bucket,
                                                read_path,
                                                extra_get_args={"Range": ''.join(
                                                    ['bytes=', str(plb), '-',
                                                     str(pub)])
                                                })

                ub = plb
                i = 0
                while (plb != 0) and (b'\n' not in chunk[:marked_pos]) and i < 5:
                    plb = plb - BOUND_EXTRACTION_MARGIN
                    # cos read of bytes=plb,pub

                    chunk = storage.get_object(read_bucket,
                                                    read_path,
                                                    extra_get_args={"Range": ''.join(
                                                        ['bytes=', str(plb), '-',
                                                         str(pub)])
                                                    })

                    marked_pos = upper_bound - plb
                    i = i + 1
                for i in reversed(range(0, marked_pos)):
                    if chunk[i:i + 1] == b'\n':
                        plb = plb + i + 1
                        break

                ub = plb - 1
            else:
                ub = pub

        return lb, ub


def part_to_IO(read_part) -> Union[BytesIO, StringIO]:

    if isinstance(read_part,str):
        read_part = StringIO(read_part)
    if isinstance(read_part, bytes):
        read_part = BytesIO(read_part)

    return read_part

def read_obj(storage: Storage, Bucket: str, Key: str,
              sufixes: List[str]) -> bytes:
    storage.get_object(bucket = Bucket,
                       key = "_".join([Key] + sufixes))
