from os.path import expanduser

MINIMUM_WORKER_MEMORY_PERC = 0.05
MAXIMUM_WORKER_MEMORY_PERC = 0.5
WORKER_NUMBER_STEP = 1
WORKER_MEMORY = 2048
PREDICTION_DATA_FILENAME = "pred"
CHUNK_SIZE = 64 * (1024) ** 2


def pred_file_path( storage:str, granularity:int = int(CHUNK_SIZE / (1024) ** 2) ):

    home = expanduser("~")

    return "{}/.lithops/{}_{}_{}.pickle".format(home, PREDICTION_DATA_FILENAME, storage, granularity)


