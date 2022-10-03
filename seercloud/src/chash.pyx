# distutils: language=c
# cython: language_level=3

import numpy as np
cimport numpy as np

ctypedef np.npy_float FLOAT
ctypedef np.npy_int INT

from libc.stdlib cimport malloc, free

cimport cython
@cython.boundscheck(False) # turn off bounds-checking for entire function
@cython.wraparound(False)  # turn off negative index wrapping for entire function
def chash(np.ndarray key_array, unsigned int row_number, unsigned int reducer_number):
    cdef int i
    # allocate number * sizeof(double) bytes of memory
    cdef unsigned int * index_array = <unsigned int *> malloc(row_number * sizeof(double))
    if not index_array:
        raise MemoryError()

    try:
        for i in range(row_number):
            index_array[i] = hash(key_array[i]) % reducer_number

        # ... let's just assume we do some more heavy C calculations here to make up
        # for the work that it takes to pack the C double values into Python float
        # objects below, right after throwing away the existing objects above.

        return [x for x in index_array[:row_number]]
    finally:
        # return the previously allocated memory to the system
        free(index_array)

