import numpy as np

is_sorted = lambda a: np.all(a[:-1] <= a[1:])