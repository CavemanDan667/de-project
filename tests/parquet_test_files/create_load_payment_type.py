import pandas as pd
from pg8000.native import Connection
import awswrangler as wr



def create_test_payment_type_parquet(file_path):

    sample_data = {
        'payment_type_id': [1, 2, 3, 4],
        'payment_type_name': ['TYPE_ONE', 'TYPE_TWO', 'TYPE_THREE', 'TYPE_FOUR']
    }

    df = pd.DataFrame(sample_data)
    df.to_parquet(path=file_path, index=False)
    read_df = pd.read_parquet(file_path)
    print(read_df)

create_test_payment_type_parquet(file_path='tests/parquet_test_files/test_pq_data')