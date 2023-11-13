import re


def extract_newest_time(filenames, table):
    """This function takes a list of filenames,
    extracts timestamps from them using regex,
    sorts them into order,
    and returns the largest one.

    Args:
        filenames(list):
            will be returned out of a function that
            queries the contents of an AWS s3 bucket.
            Each filename will consist of a string
            pseudo-directory name, followed by a forward slash,
            a string of numbers and a .csv extension.

    Returns:
        newest_time(int):
            a number representing the most recent unix time
            found in the file names.

    Raises:
        TypeError if passed an argument with value of None.
        """
    newest_time = 0
    table_files = []

    for item in filenames:
        try:
            if table == re.search("(.*)(?=\/)", item).group(): # noqa
                table_files.append(item)
        except AttributeError:
            pass

    if len(table_files) > 0:
        for file in table_files:
            try:
                file_time = int(re.search("(?<=/)([0-9]+)(?=.csv)",
                                          file).group())
                if file_time > newest_time:
                    newest_time = file_time
            except AttributeError:
                pass

    return newest_time
