import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def extract_event_data(event):
    """
    Takes an s3 put event and extracts the table name and file name.

    Args:
        event (dict): S3 PUT event object

    Raises:
        k: KeyError

    Returns:
        tuple: table_name, unix
    """
    try:
        file_name = event['Records'][0]['s3']['object']['key']
        unix = file_name.split('/')[1][:-4]
        table_name = file_name.split('/')[0]
        return table_name, unix
    except KeyError as k:
        logger.error(f'extract_event_data has raised: {k}')
        raise k
