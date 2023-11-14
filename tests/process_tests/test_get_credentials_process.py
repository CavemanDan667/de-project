from src.process.process_utils.get_credentials import get_credentials
from moto import mock_secretsmanager
import boto3
from botocore.exceptions import ClientError
import pytest


@mock_secretsmanager
def test_get_credentials_can_retrieve_secret_as_a_dictionary():
    """Test sets up mock secret to test if function can
    retrieve said secret from AWS if a valid secret name is
    passed and return result in a dictionary"""
    conn = boto3.client("secretsmanager", region_name="eu-west-2")
    conn.create_secret(
        Name="test-name",
        SecretString="""{
            "TEST_DATABASE": "item1",
            "TEST_HOST": "item2",
            "TEST_PASSWORD": "item3",
            "TEST_PORT": "item4",
            "TEST_USER": "item5" }
            """,
    )
    result = get_credentials("test-name")
    assert result == {
        "TEST_DATABASE": "item1",
        "TEST_HOST": "item2",
        "TEST_PASSWORD": "item3",
        "TEST_PORT": "item4",
        "TEST_USER": "item5",
    }


@mock_secretsmanager
def test_get_credentials_returns_ClientError_if_passed_unknown_secret_name():
    with pytest.raises(ClientError) as c:
        get_credentials("invalid_secret_name")
    err = c.value.response["Error"]
    assert err["Message"] == "Secrets Manager can't find the specified secret."
    assert err["Code"] == "ResourceNotFoundException"
