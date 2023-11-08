import pandas as pd


def transform_counterparty(csv_file, conn):
    data = pd.read_csv(csv_file)
    return data
