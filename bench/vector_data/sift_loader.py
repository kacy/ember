"""
SIFT1M dataset loader.

reads .fvecs and .ivecs binary formats used by the texmex corpus.
format: each vector is prefixed by a 4-byte int32 dimension, followed by
dim float32 (fvecs) or int32 (ivecs) values.
"""

import numpy as np
import os


def read_fvecs(filename: str) -> np.ndarray:
    """read vectors from .fvecs file format."""
    with open(filename, "rb") as f:
        data = np.fromfile(f, dtype=np.float32)
    dim = int(data[0].view(np.int32))
    # each vector is (1 + dim) float32s: dimension prefix + values
    return data.reshape(-1, dim + 1)[:, 1:].copy()


def read_ivecs(filename: str) -> np.ndarray:
    """read vectors from .ivecs file format."""
    with open(filename, "rb") as f:
        data = np.fromfile(f, dtype=np.int32)
    dim = int(data[0])
    return data.reshape(-1, dim + 1)[:, 1:].copy()


def load_sift1m(data_dir: str):
    """
    load the SIFT1M dataset.

    returns:
        base_vectors:   (1000000, 128) float32 array
        query_vectors:  (10000, 128) float32 array
        ground_truth:   (10000, 100) int32 array of nearest neighbor indices
    """
    sift_dir = os.path.join(data_dir, "sift")
    base = read_fvecs(os.path.join(sift_dir, "sift_base.fvecs"))
    queries = read_fvecs(os.path.join(sift_dir, "sift_query.fvecs"))
    ground_truth = read_ivecs(os.path.join(sift_dir, "sift_groundtruth.ivecs"))
    return base, queries, ground_truth
