import logging
import os
from datetime import datetime, timedelta
from databasewrite import execute_db_operation


def delete_old_files(cursor, timezone, days_old=30, aws_upload=True, gcp_upload=True):
    """
    Deletes old files from local storage based on their upload status and last modification date.

    Args:
    cursor (sqlite3.Cursor): The database cursor used to execute queries.
    timezone (pytz.timezone): The timezone to use for calculating the cutoff date.
    days_old (int, optional): The number of days old files must be to be deleted (default is 30).
    aws_upload (bool, optional): Whether to include files uploaded to AWS S3 in the deletion criteria (default is True).
    gcp_upload (bool, optional): Whether to include files uploaded to GCS in the deletion criteria (default is True).

    Returns:
    None

    This function calculates the cutoff date for file deletion, queries the database to find files that are
    older than the cutoff date and meet the upload status criteria, deletes these files from local storage,
    and updates the database record to reflect the deletion.

    Example:
    >>> delete_old_files(cursor, pytz.utc, days_old=30, aws_upload=True, gcp_upload=True
    """
    # Calculate the cutoff date (days_old days ago)
    cutoff_date = datetime.now(timezone) - timedelta(days=days_old)
    cutoff_date = cutoff_date.replace(hour=0, minute=0, second=0, microsecond=0)
    cutoff_date_str = cutoff_date.strftime('%Y-%m-%d')

    # Construct the query based on the flags
    query = '''
        SELECT filename, localdestination
        FROM filestatus
        WHERE localstatus = 1
          AND datetime < ?
    '''
    conditions = []

    if aws_upload:
        conditions.append('AWSstatus = 1')
        conditions.append('dataintegrityAWS = 1')
    if gcp_upload:
        conditions.append('GCPstatus = 1')
        conditions.append('dataintegrityGCP = 1')

    if conditions:
        query += ' AND ' + ' AND '.join(conditions)

    cursor.execute(query, (cutoff_date_str,))
    files_to_delete = cursor.fetchall()

    logging.info(f"Deleting {len(files_to_delete)} files from local storage")

    for filename, localdestination in files_to_delete:
        try:
            # Remove the local file
            if os.path.exists(localdestination):
                os.remove(localdestination)
                logging.info(f"Deleted local file: {localdestination}")
            else:
                logging.warning(f"Local file does not exist: {localdestination}")
        except OSError as e:
            logging.error(f"Error deleting file {localdestination}: {e}")

        # Update the database record
        def update_db_record(cursor, filename):
            cursor.execute('''
                UPDATE filestatus
                SET localstatus = 0,
                    localdestination = NULL
                WHERE filename = ?
            ''', (filename,))

        execute_db_operation(update_db_record, cursor, filename)
