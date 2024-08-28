import sqlite3
import logging
import time
from network import send_email
from utils import extract_datetime_from_filename


def initialize_database(db_path):
    """
    Initializes the SQLite database at the specified path and creates the necessary table if it doesn't exist.

    Args:
    db_path (str): The file path of the SQLite database to connect to or create.

    The function does the following:
    - Enables Write-Ahead Logging (WAL) mode for better concurrency and performance.
    - Creates a table named 'filestatus' if it doesn't already exist.
    - Defines columns for tracking file statuses across different storage services (GCP, AWS, NAS, etc.),
      their destinations, data integrity, and local file information.
    """
    # Connect to the SQLite database at the provided path
    with sqlite3.connect(db_path) as conn:
        # Enable Write-Ahead Logging (WAL) mode to improve performance and allow concurrent reads and writes
        conn.execute('PRAGMA journal_mode=WAL;')

        # Create a cursor object to interact with the database
        cursor = conn.cursor()

        # Create the 'filestatus' table if it doesn't already exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS filestatus (
            ID INTEGER PRIMARY KEY AUTOINCREMENT,  -- Unique ID for each file entry
            filename TEXT NOT NULL UNIQUE,  -- The name of the file (must be unique)
            GCPstatus INTEGER NOT NULL DEFAULT 0,  -- Status of file upload to Google Cloud Platform (0 = not uploaded, 1 = uploaded)
            AWSstatus INTEGER NOT NULL DEFAULT 0,  -- Status of file upload to Amazon Web Services (0 = not uploaded, 1 = uploaded)
            NASstatus INTEGER NOT NULL DEFAULT 0,  -- Status of file copy to Network-Attached Storage (0 = not copied, 1 = copied)
            GCPdestination TEXT,  -- The destination path of the file on GCP (optional)
            AWSdestination TEXT,  -- The destination path of the file on AWS (optional)
            filetype TEXT NOT NULL,  -- The type of the file (e.g., image, video, etc.)
            AWSbucketname TEXT,  -- The name of the AWS S3 bucket where the file is stored (optional)
            GCPbucketname TEXT,  -- The name of the GCP storage bucket where the file is stored (optional)
            localstatus INTEGER NOT NULL DEFAULT 1,  -- Status of file on local storage (1 = exists, 0 = deleted)
            localdestination TEXT,  -- The local path where the file is stored (optional)
            dataintegrityAWS INTEGER NOT NULL DEFAULT 0,  -- Data integrity status on AWS (0 = not verified, 1 = verified)
            dataintegrityGCP INTEGER NOT NULL DEFAULT 0,  -- Data integrity status on GCP (0 = not verified, 1 = verified)
            datetime TEXT  -- Datetime extracted from the filename (optional)
        )
        ''')

        # Commit the transaction to save changes to the database
        conn.commit()


def execute_db_operation(operation, cursor, *args, retries=5, delay=1):
    """
    Executes a database operation with retries if the database is locked or an operational error occurs.

    Args:
    operation (function): The database operation function to be executed.
    cursor (sqlite3.Cursor): The database cursor used to execute the operation.
    *args: Arguments to pass to the operation function.
    retries (int, optional): The number of retries if the operation fails due to a locked database (default is 5).
    delay (int, optional): The delay in seconds between retries if the database is locked (default is 1 second).

    Returns:
    None

    The function attempts to execute the provided database operation and commits the transaction.
    If the database is locked, it retries the operation up to the specified number of retries.
    If an exception occurs, it logs the error, sends an email notification, and stops further attempts.
    """
    for attempt in range(retries):
        try:
            # Attempt to execute the provided operation with the given arguments
            operation(cursor, *args)

            # Commit the transaction to save the changes in the database
            cursor.connection.commit()
            return  # Exit the function if the operation is successful

        except sqlite3.OperationalError as e:
            # Check if the error is due to the database being locked
            if "database is locked" in str(e):
                # Log a warning and wait before retrying
                logging.warning(f"Database is locked. Retrying in {delay} seconds... (Attempt {attempt + 1}/{retries})")
                time.sleep(delay)
            else:
                # Log the error and stop further attempts if it's a different operational error
                logging.error(f"Database operation failed: {e}.")
                return

        except Exception as e:
            # Log the error and send an email notification for any non-operational errors
            logging.error(f"An error occurred during the database operation: {e}.")
            subject = 'An error occurred during the database operation'
            body = f"An error occurred during the database operation: {e}."
            send_email(subject, body)
            return

    # If the maximum number of retries is reached, log the failure and send an email notification
    logging.error("Maximum retries reached. Database operation failed.")
    subject = 'Database Operation Failed'
    body = "An error occurred during the database operation. Maximum retries reached."
    send_email(subject, body)
    return


def insert_file_record(cursor, filename, filetype, local_destination):
    """
    Inserts a new file record into the 'filestatus' table.

    Args:
    cursor (sqlite3.Cursor): The database cursor used to execute the SQL command.
    filename (str): The name of the file to be inserted into the database.
    filetype (str): The type of the file (e.g., image, video).
    local_destination (str): The local path where the file is stored.

    Returns:
    None

    The function attempts to insert the file record into the database.
    If an exception occurs, it logs the error and sends an email notification.
    """
    try:
        # Extract the datetime from the filename using the custom function extract_datetime_from_filename
        file_datetime, _, _ = extract_datetime_from_filename(filename)

        # Insert the new file record into the 'filestatus' table
        cursor.execute('''
            INSERT INTO filestatus (filename, filetype, localstatus, localdestination, datetime)
            VALUES (?, ?, 1, ?, ?)
        ''', (filename, filetype, local_destination, file_datetime))

    except Exception as e:
        # Log the error and prepare an email notification in case of an exception
        logging.error(f"An error occurred while inserting file record: {e}")
        subject = 'An error occurred while inserting file record'
        body = f"An error occurred while inserting file record: {e}"
        send_email(subject, body)


def update_file_record_aws(cursor, filename, aws_destination, bucket_name, data_integrityAWS):
    """
    Updates the AWS-related fields of a file record in the 'filestatus' table.

    Args:
    cursor (sqlite3.Cursor): The database cursor used to execute the SQL command.
    filename (str): The name of the file whose record needs to be updated.
    aws_destination (str): The destination path of the file in AWS S3.
    bucket_name (str): The name of the AWS S3 bucket where the file is stored.
    data_integrityAWS (int): The status of data integrity verification on AWS (0 = not verified, 1 = verified).

    Returns:
    None

    This function updates the AWS-related status fields for the given filename in the database.
    It sets:
    - `AWSstatus` to 1 (indicating the file has been uploaded to AWS),
    - `AWSdestination` to the given path in AWS,
    - `AWSbucketname` to the given S3 bucket name,
    - `dataintegrityAWS` to the specified data integrity status.

    If an exception occurs, it logs the error and sends an email notification.
    """
    try:
        # Execute the SQL command to update the AWS-related fields for the given filename
        cursor.execute('''
        UPDATE filestatus
        SET AWSstatus = ?, AWSdestination = ?, AWSbucketname = ?, dataintegrityAWS = ?
        WHERE filename = ?
        ''', (1, aws_destination, bucket_name, data_integrityAWS, filename))

    except Exception as e:
        # Log the error and prepare an email notification in case of an exception
        logging.error(f"An error occurred while updating file record AWS status: {e}")
        subject = 'An error occurred while updating file record (AWS)'
        body = f"An error occurred while updating file record (AWS): {e}"
        send_email(subject, body)


def update_file_record_gcp(cursor, filename, gcp_destination, bucket_name, data_integrityGCP):
    """
    Updates the GCP-related fields of a file record in the 'filestatus' table.

    Args:
    cursor (sqlite3.Cursor): The database cursor used to execute the SQL command.
    filename (str): The name of the file whose record needs to be updated.
    gcp_destination (str): The destination path of the file in GCP (Google Cloud Platform).
    bucket_name (str): The name of the GCP bucket where the file is stored.
    data_integrityGCP (int): The status of data integrity verification on GCP (0 = not verified, 1 = verified).

    Returns:
    None

    This function updates the GCP-related status fields for the given filename in the database.
    It sets:
    - `GCPstatus` to 1 (indicating the file has been uploaded to GCP),
    - `GCPdestination` to the given path in GCP,
    - `GCPbucketname` to the given bucket name in GCP,
    - `dataintegrityGCP` to the specified data integrity status.

    If an exception occurs, it logs the error and sends an email notification.
    """
    try:
        # Execute the SQL command to update the GCP-related fields for the given filename
        cursor.execute('''
        UPDATE filestatus
        SET GCPstatus = ?, GCPdestination = ?, GCPbucketname = ?, dataintegrityGCP = ?
        WHERE filename = ?
        ''', (1, gcp_destination, bucket_name, data_integrityGCP, filename))

    except Exception as e:
        # Log the error and prepare an email notification in case of an exception
        logging.error(f"An error occurred while updating file record GCP status: {e}")
        subject = 'An error occurred while updating file record (GCP)'
        body = f"An error occurred while updating file record (GCP): {e}"
        send_email(subject, body)
