from typing import Dict, List, Tuple

from seercloud.operation import Exchange, Sort
from seercloud.operation.Partition import Partition
from seercloud.operation.groupby import Groupby
from seercloud.scheduler.stage import Stage


# Study succession of operations and type of data, and check if key-pointer and
# hashing is feasible.
def _partition_conds(stages: Dict[int, Stage], dependencies: List[Tuple[int, int]], current_stage:int) -> bool:

    cond2 = False
    partition = None
    for d in dependencies:
        if d[0] == current_stage:
            for op in stages[d[1]].operations:
                if isinstance(op, Groupby):
                    cond2 = True
                    partition = Partition.HASH
                if isinstance(op, Sort):
                    cond2 = True
                    partition = Partition.SEGMENT


    cond3 = isinstance(stages[current_stage].operations[-1], Exchange)

    if cond2 and cond3:
        stages[current_stage].partition = partition
    else:
        stages[current_stage].partition = None


def _key_pointer_conds(stages: Dict[int, Stage], dependencies: List[Tuple[int, int]], current_stage:int) -> bool:

    cond1 = False
    for d in dependencies:
        if d[0] == current_stage:
            if True in [ (isinstance(op, Groupby) or isinstance(op, Sort))
                         for op in stages[d[1]].operations]:
                for op in stages[d[1]].operations:
                    if isinstance(op, Groupby) or isinstance(op, Sort):
                        stages[current_stage].operations[0].key = op.key
                cond1 = True

    cond2 = isinstance(stages[current_stage].operations[-1], Exchange)

    cond3 = stages[current_stage].types is not None

    if cond1 and cond2 and cond3:
        stages[current_stage].do_kp = True
    else:
        stages[current_stage].do_kp = False

