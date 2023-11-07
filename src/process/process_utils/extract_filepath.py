# receives an event
# outputs S3 URI of a bucket object


def extract_filepath(event):
    """Extracts S3 URI from event JSON

    Args:
        event (dictionary): The notification message that Amazon
        S3 sends when an object is created. In JSON format.

    Returns:
        string: The S3 URI of the object referenced in the
        given event JSON. Has the form "s3://bucket/key"
    """
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    filepath = "s3://" + bucket_name + "/" + key
    return filepath
