import boto3
from botocore.exceptions import ClientError
import json


def get_credentials(secret_name):
    """Fetches database credentials from AWS Secrets Manager

    Args:
        secret_name (string): Name of the AWS Secret which contains
            the required database credentials.

    Raises:
        e: ClientError

    Returns:
        dictionary: The database credentials in a dictionary.
    """
    client = boto3.client("secretsmanager")

    try:
        response = client.get_secret_value(SecretId=secret_name)
        credentials = json.loads(response["SecretString"])
        return credentials
    except ClientError as e:
        raise e
