import logging
import base64
from network import send_email


def upload_to_gcs(local_file_path, bucket_name, gcs_blob_name, gcs_client):
    """
    Uploads a local file to a Google Cloud Storage (GCS) bucket.

    Args:
    local_file_path (str): The path to the local file to be uploaded.
    bucket_name (str): The name of the GCS bucket where the file will be uploaded.
    gcs_blob_name (str): The name of the blob (object) in the GCS bucket.
    gcs_client (google.cloud.storage.Client): An initialized GCS client object from the `google-cloud-storage` library.

    Returns:
    tuple: A tuple containing:
        - bool: True if the upload is successful, False otherwise.
        - str or None: The MD5 checksum of the uploaded file in hexadecimal format if successful, None otherwise.

    This function uploads a file to a specified GCS bucket, logs the result of the operation, and retrieves the MD5 hash of the uploaded file.
    The MD5 checksum is used for verifying the upload.

    Example:
    >>> upload_to_gcs('/path/to/file.txt', 'my-bucket', 'path/in/bucket/file.txt', gcs_client)
    (True, 'd41d8cd98f00b204e9800998ecf8427e')
    """
    try:
        # Retrieve the GCS bucket object
        bucket = gcs_client.bucket(bucket_name)

        # Create a blob (object) from the specified blob name
        blob = bucket.blob(gcs_blob_name)

        # Upload the local file to GCS
        blob.upload_from_filename(local_file_path)

        # Log a success message
        logging.info(f'Uploaded {local_file_path} to {bucket_name}/{gcs_blob_name}')

        # Reload the blob to fetch updated metadata
        blob.reload()

        # Retrieve the MD5 hash of the uploaded file
        gcs_md5 = blob.md5_hash

        # Decode the base64-encoded MD5 hash and convert to hexadecimal
        gcs_md5_bytes = base64.b64decode(gcs_md5)
        gcs_md5_hex = gcs_md5_bytes.hex()

        return True, gcs_md5_hex

    except Exception as e:
        # Log an error if the upload fails
        logging.error(f'Error uploading {local_file_path} to GCS: {e}')

        # Prepare and send an email notification with the error details
        subject = 'Upload to GCS'
        body = f"Error uploading {local_file_path} to GCS: {e}"
        send_email(subject, body)

        return False, None


def delete_object_from_gcs(bucket_name, file_name, gcs_client):
    """
    Deletes an object from a Google Cloud Storage (GCS) bucket.

    Args:
    bucket_name (str): The name of the GCS bucket from which the object will be deleted.
    file_name (str): The name of the object (blob) in the GCS bucket.
    gcs_client (google.cloud.storage.Client): An initialized GCS client object from the `google-cloud-storage` library.

    Returns:
    None

    This function uses the `google-cloud-storage` client to delete an object from a specified GCS bucket.
    It logs an info message if the deletion is successful and an error message if an exception occurs.

    Example:
    >>> delete_object_from_gcs('my-bucket', 'path/to/file.txt', gcs_client)
    Deleted path/to/file.txt from GCP bucket my-bucket.
    """
    try:
        # Get the GCS bucket object
        bucket = gcs_client.bucket(bucket_name)

        # Get the blob (object) from the bucket
        blob = bucket.blob(file_name)

        # Delete the blob from the bucket
        blob.delete()

        # Log a success message
        logging.info(f'Deleted {file_name} from GCP bucket {bucket_name}.')
    except Exception as e:
        # Log an error if the deletion fails
        logging.error(f'Error deleting {file_name} from GCP bucket {bucket_name}: {e}')
