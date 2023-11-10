import pandas as pd


def create_test_payment_type_parquet(file_path):

    sample_data = {
        'staff_id': [1],
        'first_name': ['NameA'],
        'last_name': ['SurnameA'],
        'department_name': ['Dept3'],
        'location': ['LocationB'],
        'email_address': ['namea.surnamea@terrifictotes.com']
    }

    df = pd.DataFrame(sample_data)
    df.to_parquet(path=file_path, index=False)
    read_df = pd.read_parquet(file_path)
    print(read_df)


create_test_payment_type_parquet(
    file_path='tests/parquet_test_files/staff-update-transfer.parquet')
