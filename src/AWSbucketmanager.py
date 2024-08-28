import logging
from network import send_email


def upload_to_s3(local_file_path, bucket_name, s3_file_name, s3_client):
    """
    Uploads a local file to an Amazon S3 bucket.

    Args:
    local_file_path (str): The path to the local file to be uploaded.
    bucket_name (str): The name of the S3 bucket where the file will be uploaded.
    s3_file_name (str): The key (name) for the file in the S3 bucket.
    s3_client (boto3.client): An initialized S3 client object from the `boto3` library.

    Returns:
    tuple: A tuple containing:
        - bool: True if the upload is successful, False otherwise.
        - str or None: The MD5 checksum (ETag) of the uploaded file if successful, None otherwise.

    This function uploads a file to a specified S3 bucket and logs the result of the operation.
    It retrieves the ETag (MD5 checksum) of the uploaded file from the response, which can be used for verifying the upload.

    Example:
    >>> upload_to_s3('/path/to/file.txt', 'my-bucket', 'path/in/bucket/file.txt', s3_client)
    (True, 'd41d8cd98f00b204e9800998ecf8427e')
    """
    try:
        # Open the local file in binary read mode
        with open(local_file_path, "rb") as data:
            # Upload the file to S3
            response = s3_client.put_object(Bucket=bucket_name, Key=s3_file_name, Body=data)

        # Log a success message
        logging.info(f'Uploaded {local_file_path} to {bucket_name}/{s3_file_name}')

        # Extract the ETag (MD5 checksum) from the response
        s3_md5 = response['ETag'].strip('"')

        return True, s3_md5

    except Exception as e:
        # Log an error if the upload fails
        logging.error(f'Error uploading {local_file_path} to S3: {e}')

        # Prepare and send an email notification
        subject = 'Upload to S3'
        body = f"Error uploading {local_file_path} to S3: {e}"
        send_email(subject, body)

        return False, None


def delete_object_from_s3(bucket_name, s3_file_name, s3_client):
    """
    Deletes an object from an S3 bucket.

    Args:
    bucket_name (str): The name of the S3 bucket from which the object will be deleted.
    s3_file_name (str): The key (path) of the object in the S3 bucket.
    s3_client (boto3.client): An initialized S3 client object from the `boto3` library.

    Returns:
    None

    This function uses the `boto3` S3 client to delete an object from a specified S3 bucket.
    It logs an info message if the deletion is successful and an error message if an exception occurs.

    Example:
    >>> delete_object_from_s3('my-bucket', 'path/to/file.txt', s3_client)
    Deleted path/to/file.txt from my-bucket
    """
    try:
        # Attempt to delete the specified object from the S3 bucket
        s3_client.delete_object(Bucket=bucket_name, Key=s3_file_name)
        logging.info(f'Deleted {s3_file_name} from {bucket_name}')
    except Exception as e:
        # Log an error if the deletion fails
        logging.error(f'Error deleting {s3_file_name} from S3: {e}')
