from seercloud.operation.Partition import Partition


class TaskInfo():


    surname_in: str
    surname_out: str

    do_kp: bool
    partition: Partition

    def __init__(self, task_id: int, stage_id: int, job_id: int, num_tasks: int,
                 read_path: str, read_bucket:str, write_path: str, write_bucket: str,
                 surname_in: str, surname_out: str,
                 **kwargs):

        self.task_id = task_id
        self.stage_id = stage_id
        self.job_id = job_id
        self.num_tasks = num_tasks
        self.read_path = read_path
        self.read_bucket = read_bucket
        self.write_path = write_path
        self.write_bucket = write_bucket
        self.surname_in = surname_in
        self.surname_out = surname_out
        self.__dict__.update(kwargs)



