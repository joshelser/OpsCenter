import pandas
from . import end_partition


def test_ql():
    d = [
        {'COST': 1,
         'WAREHOUSE_SIZE': 'M',
         'MODEL_RUNTIME_SCORE': 'L',
         'QUERY_TYPE': 'SELECT',
         },
        {'COST': 1.01,
         'WAREHOUSE_SIZE': 'M',
         'MODEL_RUNTIME_SCORE': 'L',
         'QUERY_TYPE': 'SELECT',
         },
        {'COST': 0.8,
         'WAREHOUSE_SIZE': 'M',
         'MODEL_RUNTIME_SCORE': 'M',
         'QUERY_TYPE': 'SELECT',
         },
    ]
    return end_partition(pandas.DataFrame(d))

