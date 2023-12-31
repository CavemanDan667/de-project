from src.transform.transform_utils.transform_staff import transform_staff
import pytest
import pandas as pd
import subprocess

identity = subprocess.check_output('whoami')

mock_dept_data = [
    [1, 'Dept1', 'LocationA'],
    [2, 'Dept2', 'LocationA'],
    [3, 'Dept3', 'LocationB'],
    [4, 'Dept4', 'LocationB']
]


@pytest.fixture
def mock_conn():
    class MockConnection:
        def __init__(self):
            self.name = 'mockconn'

        def run(*args):
            return mock_dept_data
    return MockConnection()


def test_function_returns_data_frame(mock_conn):
    result = transform_staff(
        's3://de-project-test-data/csv/test-staff.csv',
        mock_conn
        )
    assert isinstance(result, pd.core.frame.DataFrame)


def test_function_returns_correct_data(mock_conn):
    result = transform_staff(
       's3://de-project-test-data/csv/test-staff.csv',
       mock_conn
    )
    assert result.values.tolist() == [
        [1, 'NameA', 'SurnameA',
         'Dept2', 'LocationA',
         'namea.surnamea@terrifictotes.com'],
        [2, 'NameB', 'SurnameB',
         'Dept1', 'LocationA',
         'nameb.surnameb@terrifictotes.com'],
        [3, 'NameC', 'SurnameC',
         'Dept1', 'LocationA',
         'namec.surnamec@terrifictotes.com'],
        [4, 'NameD', 'SurnameD',
         'Dept3', 'LocationB',
         'named.surnamed@terrifictotes.com',],
        [5, 'NameE', 'SurnameE',
         'Dept4', 'LocationB',
         'namee.surnamee@terrifictotes.com']
    ]


def test_function_only_returns_new_data(mock_conn):
    result = transform_staff(
        's3://de-project-test-data/csv/test-staff-2.csv',
        mock_conn
    )
    assert result.values.tolist() == [[6, 'NameF', 'SurnameF',
                                       'Dept2', 'LocationA',
                                       'namef.surnamef@terrifictotes.com']]


def test_function_raises_key_error_with_non_matching_info(mock_conn):
    with pytest.raises(KeyError):
        transform_staff(
            "s3://de-project-test-data/csv/test-rogue-staff.csv",
            mock_conn
        )


def test_function_raises_value_error_with_incorrect_file(mock_conn):
    with pytest.raises(ValueError):
        transform_staff(
            "s3://de-project-test-data/csv/test-currency.csv",
            mock_conn
        )
