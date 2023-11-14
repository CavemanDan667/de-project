import pandas as pd


def create_test_parquet(file_path):

    sample_data = {
        'counterparty_id': [1, 2, 3, 4],
        'counterparty_legal_name': ['Company and Sons', "Clarke, Hunter and Lorimer", 'Company Inc', 'Another Company Inc'],
        'counterparty_legal_address_line_1': ["234 St. Steven's Road", '123 Main Street', 'Flat 12', '5 Far Lane'],
        'counterparty_legal_address_line_2': [None, None, 'Block A', 'Parkway'],
        'counterparty_legal_district': [None, 'Central', None, 'North Shore'],
        'counterparty_legal_city': ['Old Town', 'New Town', 'New South Bridge', 'Castletown'],
        'counterparty_legal_postal_code': ['22222-3333', '12345', '67890', 'AB2 3CD'],
        'counterparty_legal_country': ['Northern Ireland', 'England', 'Scotland', 'Wales'],
        'counterparty_legal_phone_number': ['07700 100200', '1234 567890', '0044 123456', '1234 800900']
    }

    df = pd.DataFrame(sample_data)
    df.to_parquet(path=file_path, index=False)


create_test_parquet(file_path='tests/parquet_test_files/test-counterparty.parquet')
