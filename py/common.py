import numpy as np
import pandas as pd
from sklearn import preprocessing
from pyarrow import csv
import pyarrow.parquet as pq


def get_data(path):
    data = pq.read_pandas(path).to_pandas()
    return data


def save_data(data, path):
    data.to_parquet(path, engine='pyarrow', index=False)
