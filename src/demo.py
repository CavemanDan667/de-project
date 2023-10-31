import logging
import tempfile

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handler(event, context):
    logger.info("Creating a file locally")
    create_text_file()

# Create a temporary file
# temp = tempfile.NamedTemporaryFile()
# temp.write(b'This is a test')
# temp.seek(0)
# print(temp.read())


def create_text_file():
    file_name = 'test.txt'
    with open(file_name, "w") as file:
        file.write("This is a test")


create_text_file()
