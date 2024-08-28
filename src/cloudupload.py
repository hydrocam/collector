import logging
from utils import extract_datetime_from_filename, data_integrity_check
from AWSbucketmanager import upload_to_s3, delete_object_from_s3
from GCSbucketmanager import upload_to_gcs, delete_object_from_gcs
from network import send_email
from databasewrite import execute_db_operation, update_file_record_aws, update_file_record_gcp
from databaseread import get_unuploaded_files, is_uploaded_to_aws, is_uploaded_to_gcp
def upload_files_to_cloud(file_path, file_name, bucket_name_aws, bucket_name_gcp, aws_upload, gcp_upload, s3_client,
                          gcs_client):
    """
    Uploads a file to AWS S3 and/or Google Cloud Storage (GCS) and verifies the upload integrity.

    Args:
    file_path (str): The local path of the file to be uploaded.
    file_name (str): The name of the file to be used in the cloud storage.
    bucket_name_aws (str): The name of the AWS S3 bucket.
    bucket_name_gcp (str): The name of the GCS bucket.
    aws_upload (bool): Whether to upload the file to AWS S3 (default is True).
    gcp_upload (bool): Whether to upload the file to GCS (default is True).
    s3_client (boto3.client): An initialized AWS S3 client object.
    gcs_client (google.cloud.storage.Client): An initialized GCS client object.

    Returns:
    tuple: A tuple containing:
        - bool: True if the file was successfully uploaded to AWS S3, False otherwise.
        - bool: True if the file integrity was confirmed for AWS S3.
        - bool: True if the file was successfully uploaded to GCS, False otherwise.
        - bool: True if the file integrity was confirmed for GCS.

    This function uploads a file to the specified cloud storage services (AWS S3 and/or GCS), checks the integrity
    of the uploaded file using MD5 checksums, and retries the upload if necessary. The file path is organized
    in a folder structure based on the extracted year and month from the file name.

    Example:
    >>> upload_files_to_cloud('/path/to/file.txt', 'file.txt', 'aws-bucket', 'gcs-bucket', True, True, s3_client, gcs_client)
    (True, True, True, True)
    """

    aws_uploaded = gcp_uploaded = False
    dataintegrityAWS = dataintegrityGCP = False

    # Handle AWS S3 upload with retries
    if aws_upload:
        attempts = 0
        _, year, month = extract_datetime_from_filename(file_name)
        aws_path = f"{year}/{month}/{file_name}"

        while attempts < 5:
            aws_uploaded, s3_md5 = upload_to_s3(file_path, bucket_name_aws, aws_path, s3_client)

            if aws_uploaded:
                dataintegrityAWS = data_integrity_check(file_path, s3_md5)
                if dataintegrityAWS:
                    logging.info(f'Successfully uploaded {file_name} to AWS on attempt {attempts + 1} with fidelity')
                    break
                else:
                    delete_object_from_s3(bucket_name_aws, aws_path, s3_client)
                    logging.info(f'Upload failed for AWS, retrying {file_name} (attempt {attempts + 1})')
            else:
                logging.info(f'Upload failed for AWS, retrying {file_name} (attempt {attempts + 1})')

            attempts += 1

        if not aws_uploaded:
            logging.error(f'Failed to upload {file_name} to AWS after 5 attempts.')
            if dataintegrityAWS is False:
                subject = 'AWS Data Integrity Issue'
                body = f'Failed to ensure data integrity for {file_name} after 5 attempts.'
                send_email(subject, body)

    # Handle GCS upload with retries
    if gcp_upload:
        attempts = 0
        _, year, month = extract_datetime_from_filename(file_name)
        gcs_path = f"{year}/{month}/{file_name}"

        while attempts < 5:
            gcp_uploaded, gcs_md5 = upload_to_gcs(file_path, bucket_name_gcp, gcs_path, gcs_client)

            if gcp_uploaded:
                dataintegrityGCP = data_integrity_check(file_path, gcs_md5)
                if dataintegrityGCP:
                    logging.info(f'Successfully uploaded {file_name} to GCP on attempt {attempts + 1} with fidelity')
                    break
                else:
                    delete_object_from_gcs(bucket_name_gcp, gcs_path, gcs_client)
                    logging.info(f'Upload failed for GCP, retrying {file_name} (attempt {attempts + 1})')
            else:
                logging.info(f'Upload failed for GCP, retrying {file_name} (attempt {attempts + 1})')

            attempts += 1

        if not gcp_uploaded:
            logging.error(f'Failed to upload {file_name} to GCP after 5 attempts.')
            if dataintegrityGCP is False:
                subject = 'GCS Data Integrity Issue'
                body = f'Failed to ensure data integrity for {file_name} after 5 attempts.'
                send_email(subject, body)

    return aws_uploaded, dataintegrityAWS, gcp_uploaded, dataintegrityGCP


def upload_unuploaded_files(cursor, AWS_image_bucket_name, AWS_video_bucket_name, GCP_image_bucket_name,
                            GCP_video_bucket_name, s3_client, gcs_client, aws_upload=True, gcp_upload=True):
    """
    Uploads unuploaded files from local storage to AWS S3 and/or Google Cloud Storage (GCS), and updates the database.

    Args:
    cursor (sqlite3.Cursor): The SQLite cursor used for database operations.
    AWS_image_bucket_name (str): The name of the AWS S3 bucket for images.
    AWS_video_bucket_name (str): The name of the AWS S3 bucket for videos.
    GCP_image_bucket_name (str): The name of the GCS bucket for images.
    GCP_video_bucket_name (str): The name of the GCS bucket for videos.
    s3_client (boto3.client): An initialized AWS S3 client object.
    gcs_client (google.cloud.storage.Client): An initialized GCS client object.
    aws_upload (bool): Flag to indicate whether to upload files to AWS S3. Defaults to True.
    gcp_upload (bool): Flag to indicate whether to upload files to GCS. Defaults to True.

    Returns:
    None

    This function:
    1. Retrieves unuploaded files from the database.
    2. Uploads these files to AWS S3 and/or GCS based on the provided flags.
    3. Updates the database with the upload status and data integrity information.

    Example:
    >>> upload_unuploaded_files(cursor, 'aws-images', 'aws-videos', 'gcp-images', 'gcp-videos', s3_client, gcs_client)
    """

    # Fetch unuploaded files based on the AWS and GCP upload flags
    unuploaded_files = get_unuploaded_files(cursor, aws_upload=aws_upload, gcp_upload=gcp_upload)
    logging.info(f"Detected {len(unuploaded_files)} unuploaded files in local storage")

    for filename, filetype, localdestination in unuploaded_files:
        try:
            # Extract year and month from the filename for folder structure
            _, year, month = extract_datetime_from_filename(filename)

            if year and month:
                # Determine the appropriate AWS bucket and upload the file if required
                if aws_upload and not is_uploaded_to_aws(cursor, filename):
                    bucket_name_aws = AWS_image_bucket_name if filetype == 'image' else AWS_video_bucket_name
                    aws_destination = f"{year}/{month}/{filename}"
                    aws_uploaded, dataintegrityAWS, _, _ = upload_files_to_cloud(
                        file_path=localdestination,
                        file_name=aws_destination,
                        bucket_name_aws=bucket_name_aws,
                        bucket_name_gcp=None,
                        aws_upload=True,
                        gcp_upload=False,
                        s3_client=s3_client,
                        gcs_client=None
                    )
                    # Update the database with the AWS upload status
                    if aws_uploaded:
                        execute_db_operation(
                            update_file_record_aws,
                            cursor,
                            filename,
                            aws_destination,
                            bucket_name_aws,
                            dataintegrityAWS
                        )

                # Determine the appropriate GCS bucket and upload the file if required
                if gcp_upload and not is_uploaded_to_gcp(cursor, filename):
                    bucket_name_gcp = GCP_image_bucket_name if filetype == 'image' else GCP_video_bucket_name
                    gcs_destination = f"{year}/{month}/{filename}"
                    _, _, gcp_uploaded, dataintegrityGCP = upload_files_to_cloud(
                        file_path=localdestination,
                        file_name=gcs_destination,
                        bucket_name_aws=None,
                        bucket_name_gcp=bucket_name_gcp,
                        aws_upload=False,
                        gcp_upload=True,
                        s3_client=None,
                        gcs_client=gcs_client
                    )
                    # Update the database with the GCP upload status
                    if gcp_uploaded:
                        execute_db_operation(
                            update_file_record_gcp,
                            cursor,
                            filename,
                            gcs_destination,
                            bucket_name_gcp,
                            dataintegrityGCP
                        )

        except Exception as e:
            # Log any errors encountered during the upload process
            logging.error(f"An error occurred while uploading {filename}: {e}")