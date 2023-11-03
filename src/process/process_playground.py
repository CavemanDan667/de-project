import pandas as pd
import numpy as np


transaction_df = pd.read_csv("tests/csv_test_files/test-transaction.csv")
purchase_df = pd.read_csv("tests/csv_test_files/test-purchase-order.csv")
sales_df = pd.read_csv("tests/csv_test_files/test-sales-order.csv")
print(transaction_df)
print(purchase_df)
print(sales_df)

purchase_transactions_df = pd.merge(transaction_df, purchase_df, on="purchase_order_id")

sales_transactions_df = pd.merge(transaction_df, sales_df, on="sales_order_id")

print(purchase_transactions_df)
print(sales_transactions_df)