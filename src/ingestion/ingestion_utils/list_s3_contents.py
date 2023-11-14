import boto3


def get_client(service_name):
    client = boto3.client(service_name)
    return client


def list_contents(bucket_name):
    """
    This function takes a bucket name as an argument
    and returns a list with all contents of that bucket.

    Args:
        bucket_name as a string.

    Returns:
        a list of the names of all objects in the s3 bucket as strings,
        or an empty list.

    """
    try:
        s3 = get_client("s3")
        object_list = s3.list_objects_v2(Bucket=bucket_name)
        items = [item["Key"] for item in object_list["Contents"]]
        return items
    except (AttributeError, KeyError):
        return []
