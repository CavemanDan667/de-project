# flake8: noqa

import pandas as pd

# transaction_df = pd.read_csv("tests/csv_test_files/test-transaction.csv")
# purchase_df = pd.read_csv("tests/csv_test_files/test-purchase-order.csv")
# sales_df = pd.read_csv("tests/csv_test_files/test-sales-order.csv")
staff_df = pd.read_csv("tests/csv_test_files/test-staff.csv", usecols = ['staff_id', 'first_name','last_name', 'email_address', 'department_id'])
department_df = pd.read_csv("tests/csv_test_files/test-department.csv", usecols = ['department_id', 'department_name', 'location'])
# print(transaction_df)
# print(purchase_df)
# print(sales_df)

# purchase_transactions_df = pd.merge(transaction_df,
#                                     purchase_df, on="purchase_order_id")

# sales_transactions_df = pd.merge(transaction_df,
#                                  sales_df, on="sales_order_id")

# print(purchase_transactions_df)
# print(sales_transactions_df)

staff_department_df = pd.merge(department_df, staff_df, on="department_id")
new_staff_department_df=staff_department_df.drop(columns='department_id')
# print(staff_department_df)

print (staff_df)
print(department_df)
print(new_staff_department_df)