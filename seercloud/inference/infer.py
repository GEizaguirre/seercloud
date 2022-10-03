import os
import pickle

import logging

import numpy as np

from seercloud.inference.config import *

logger = logging.getLogger(__name__)

def search_optimal(dataset_size: int, config: dict()):

    # Dataset size in bytes.
    # Worker memory in Megabytes.
    worker_memory = WORKER_MEMORY

    # We establish the minimum and maximum worker number
    # to analyze as multiples of the worker number
    # step.
    minimum_number_worker = int((dataset_size / \
                                 (worker_memory * 1024 ** 2
                                  * MAXIMUM_WORKER_MEMORY_PERC))
                                / WORKER_NUMBER_STEP) \
                            * WORKER_NUMBER_STEP
    minimum_number_worker = max(minimum_number_worker, 3)
    maximum_number_worker = int((dataset_size / \
                                 (worker_memory * 1024 ** 2
                                  * MINIMUM_WORKER_MEMORY_PERC))
                                / WORKER_NUMBER_STEP) \
                            * WORKER_NUMBER_STEP
    maximum_number_worker = max(min(maximum_number_worker, 1000), 3)

    #  print("{} - {}".format(minimum_number_worker, maximum_number_worker))


    # Check if equation info exists
    if not os.path.isfile(pred_file_path("ibm_cos")):
        logger.error("There is no parameter file for the worker number inferencer.")
        logger.error("Execute setup_shuffle_model to configurate the system.")
        exit(-1)

    # Load equation info
    with open(pred_file_path("ibm_cos"), 'rb') as f:
        equation_info = pickle.load(f)

    bandwidth_read = equation_info['bandwidth']['read'] * 1024 * 1024
    bandwidth_write = equation_info['bandwidth']['write'] * 1024 * 1024
    throughput_read = equation_info['throughput']['read']
    throughput_write = equation_info['throughput']['write']

    # Apply Locus equations to each worker number
    predicted_times = {
        w: eq_complete(dataset_size, w, bandwidth_read, bandwidth_write, throughput_read, throughput_write)
        for w in range(minimum_number_worker,
                       maximum_number_worker + 1,
                       WORKER_NUMBER_STEP)
    }
    optimal_worker_number = min(predicted_times, key=predicted_times.get)
    # print("Predicted workers for total operation {}".format(optimal_worker_number))

    predicted_times_shuffle = {
        w: eq_shuffle(dataset_size, w, bandwidth_read, bandwidth_write, throughput_read, throughput_write)
        for w in range(minimum_number_worker,
                       maximum_number_worker + 1,
                       WORKER_NUMBER_STEP)
    }

    # print(predicted_times)
    optimal_worker_number_shuffle = min(predicted_times_shuffle, key=predicted_times_shuffle.get)
    # print("Predicted workers for shuffle {}".format(optimal_worker_number_shuffle))

    return optimal_worker_number_shuffle


def eq_complete(D, p, bandwidth_read, bandwidth_write, throughput_read, throughput_write):
    chunk_size = 64.0 * 1024 ** 2
    return max(D / (bandwidth_read * p), np.ceil(D / (chunk_size * p)) * p / throughput_read) + \
           max(D / (bandwidth_write * p), np.ceil(D / (chunk_size * p ** 2)) * p ** 2 / throughput_write) + \
           max(D / (bandwidth_read * p), np.ceil(D / (chunk_size * p ** 2)) * p ** 2 / throughput_read) + \
           max(D / (bandwidth_write * p), np.ceil(D / (chunk_size * p)) * p / throughput_write)


def eq_shuffle(D, p, bandwidth_read, bandwidth_write, throughput_read, throughput_write):
    chunk_size = 64.0 * 1024 ** 2
    return max(D / (bandwidth_write * p), np.ceil(D / (chunk_size * p ** 2)) * p ** 2 / throughput_write) + \
           max(D / (bandwidth_read * p), np.ceil(D / (chunk_size * p ** 2)) * p ** 2 / throughput_read)