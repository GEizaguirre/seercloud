from seercloud.scheduler import Job

# Study succesion of operations and type of data, and check if key-pointer and
# hashing is feasible.
def _hash_conds(job: Job, current_stage:int, current_operation:int) -> bool:

    # cond1 = isinstance(next_operation, Groupby)

    # return cond1
    return False


def _key_pointer_conds(job: Job, current_stage:int, current_operation:int) -> bool:

    # cond1 = self.data_info.types is not None
    # cond2 = isinstance(next_operation, Sort) or isinstance(next_operation, Groupby)

    # return cond1 and cond2
    return False

