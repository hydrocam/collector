import logging


def get_unuploaded_files(cursor, aws_upload=True, gcp_upload=True):
    """
    Retrieves a list of files that have not been uploaded to specified cloud platforms.

    Args:
    cursor (sqlite3.Cursor): The database cursor for executing SQL queries.
    aws_upload (bool): Whether to check for files not uploaded to AWS (default is True).
    gcp_upload (bool): Whether to check for files not uploaded to GCP (default is True).

    Returns:
    list: A list of tuples, each containing the filename, filetype, and localdestination of unuploaded files.

    This function builds and executes a SQL query to fetch files from the database that have not been uploaded
    to the specified cloud platforms. The conditions for fetching are based on the `aws_upload` and `gcp_upload`
    flags. If neither flag is set to True, it returns an empty list.

    Example:
    >>> get_unuploaded_files(cursor, aws_upload=True, gcp_upload=False)
    [('file1.jpg', 'image', '/local/path/file1.jpg'), ('file2.mp4', 'video', '/local/path/file2.mp4')]
    """
    try:
        # Base SQL query
        query = '''
            SELECT filename, filetype, localdestination
            FROM filestatus
            WHERE 
        '''

        # List to hold conditions for the WHERE clause
        conditions = []

        # Append conditions based on whether AWS or GCP upload checks are enabled
        if aws_upload:
            conditions.append("AWSstatus = 0")
        if gcp_upload:
            conditions.append("GCPstatus = 0")

        # If there are conditions, join them with OR to fetch files not uploaded to either AWS or GCP
        if conditions:
            query += ' OR '.join(conditions)
        else:
            # No cloud platforms selected for checking
            logging.info("No cloud platform selected for checking unuploaded files.")
            return []

        # Execute the query
        cursor.execute(query)

        # Fetch all unuploaded files
        unuploaded_files = cursor.fetchall()
        logging.info(f"Fetched {len(unuploaded_files)} unuploaded files.")

        return unuploaded_files

    except Exception as e:
        # Log an error if the query execution fails
        logging.error(f"An error occurred while fetching unuploaded files: {e}")
        return []


def is_uploaded_to_aws(cursor, filename):
    """
    Checks if a file has been uploaded to AWS by querying the database.

    Args:
    cursor (sqlite3.Cursor): The database cursor used to execute queries.
    filename (str): The name of the file to check.

    Returns:
    bool: True if the file is uploaded to AWS (AWSstatus = 1), False otherwise.

    This function queries the database to determine if the file has been uploaded to AWS S3 by checking
    the `AWSstatus` field in the `filestatus` table. It logs a warning if no record is found and an error if
    an exception occurs.

    Example:
    >>> is_uploaded_to_aws(cursor, 'example_file.txt')
    True
    """
    try:
        cursor.execute('''
            SELECT AWSstatus
            FROM filestatus
            WHERE filename = ?
        ''', (filename,))
        result = cursor.fetchone()
        if result:
            return result[0] == 1
        else:
            logging.warning(f"No record found for filename: {filename}")
            return False
    except Exception as e:
        logging.error(f"An error occurred while checking if the file is uploaded to AWS: {e}")
        return False


def is_uploaded_to_gcp(cursor, filename):
    """
    Checks if a file has been uploaded to Google Cloud Platform (GCP) by querying the database.

    Args:
    cursor (sqlite3.Cursor): The database cursor used to execute queries.
    filename (str): The name of the file to check.

    Returns:
    bool: True if the file is uploaded to GCP (GCPstatus = 1), False otherwise.

    This function queries the database to determine if the file has been uploaded to GCP by checking
    the `GCPstatus` field in the `filestatus` table. It logs a warning if no record is found and an error if
    an exception occurs.

    Example:
    >>> is_uploaded_to_gcp(cursor, 'example_file.txt')
    True
    """
    try:
        cursor.execute('''
            SELECT GCPstatus
            FROM filestatus
            WHERE filename = ?
        ''', (filename,))
        result = cursor.fetchone()
        if result:
            return result[0] == 1
        else:
            logging.warning(f"No record found for filename: {filename}")
            return False
    except Exception as e:
        logging.error(f"An error occurred while checking if the file is uploaded to GCP: {e}")
        return False
