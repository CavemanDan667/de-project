from src.ingestion.ingestion_utils.get_client import get_client


def list_contents(bucket_name):
    """
    This function takes a bucket name as an argument and returns a list with all contents of that bucket

    Args:
    bucket_name as a string

    Returns:
    list of all objects in the s3 bucket as strings

    Raises:
    KeyError - If the bucket has no objects then return an empty list
    AttributeError - If the connection fails the return an empty list

    """
    try:
        s3 = get_client("s3")
        object_list = s3.list_objects_v2(Bucket=bucket_name)
        items = [item["Key"] for item in object_list["Contents"]]
        return items
    except (AttributeError, KeyError) as e:
        return []
