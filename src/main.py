import configparser
import pytz
import os
import boto3
import sqlite3
import logging
import time
from databasewrite import (initialize_database, execute_db_operation, insert_file_record, update_file_record_gcp,
                           update_file_record_aws)
from network import send_email, check_internet_connectivity, disconnect_current_wifi
from cloudupload import upload_files_to_cloud, upload_unuploaded_files
from utils import get_next_capture_time, extract_datetime_from_filename
from Capture import capture_image, capture_video
from google.cloud import storage
from datetime import datetime
from Storagecleanup import delete_old_files


def main_loop():
    # Read configuration from config.ini
    config = configparser.ConfigParser()
    config.read('config.ini')
    mst = pytz.timezone('Etc/GMT+7')
    log_directory = 'logs'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    aws_upload = config.getboolean('platform', 'aws')
    gcp_upload = config.getboolean('platform', 'gcp')

    AWS_image_bucket_name = config['aws']['image_bucket_name'] if aws_upload else None
    AWS_video_bucket_name = config['aws']['video_bucket_name'] if aws_upload else None
    s3_client = boto3.client('s3',
                             aws_access_key_id=config['aws']['aws_access_key_id'],
                             aws_secret_access_key=config['aws']['aws_secret_access_key']) if aws_upload else None

    GCP_image_bucket_name = config['gcp']['image_bucket_name'] if gcp_upload else None
    GCP_video_bucket_name = config['gcp']['video_bucket_name'] if gcp_upload else None
    gcs_client = storage.Client.from_service_account_json(config['gcp']['service_account_json']) if gcp_upload else None

    rtsp_url = config['camera']['address']

    # Directories and database path
    image_base_directory = config['directories']['image_base_directory']
    video_base_directory = config['directories']['video_base_directory']
    db_path = config['database']['db_path']

    # Run the database initialization
    initialize_database(db_path=db_path)
    current_day = datetime.now(mst).strftime('%Y-%m-%d')
    log_file = os.path.join(log_directory, f'app_{current_day}.log')
    logging.basicConfig(filename=log_file, level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    while True:
        today = datetime.now(mst).strftime('%Y-%m-%d')
        if today != current_day:
            for handler in logging.root.handlers[:]:
                logging.root.removeHandler(handler)
            log_file = os.path.join(log_directory, f'app_{today}.log')
            logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
            current_day = today

        now = datetime.now(mst)
        # Wait until the next capture time
        next_capture_time = get_next_capture_time(mst=mst)
        sleep_duration = (next_capture_time - datetime.now(mst)).total_seconds()
        logging.info(f"Sleeping for {sleep_duration} seconds")
        time.sleep(sleep_duration)

        # Capture Image and Video
        image_path, image_filename = capture_image(rtsp_url, image_base_directory, timezone=mst)
        video_path, video_filename = capture_video(rtsp_url, video_base_directory, mst, duration=40)

        time.sleep(20)

        # Extract year and month from the filename for folder structure
        _, year, month = extract_datetime_from_filename(image_filename)

        try:
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()

                # Insert records into the database
                execute_db_operation(insert_file_record, cursor, image_filename, 'image', image_path)
                execute_db_operation(insert_file_record, cursor, video_filename, 'video', video_path)

                # Upload paths for S3 and GCS
                s3_image_path = f"{year}/{month}/{image_filename}"
                s3_video_path = f"{year}/{month}/{video_filename}"
                gcs_image_path = f"{year}/{month}/{image_filename}"
                gcs_video_path = f"{year}/{month}/{video_filename}"

                # Check for internet connectivity
                if not check_internet_connectivity():
                    print("No Internet connectivity. Reconnecting to Wi-Fi...")
                    disconnect_current_wifi()
                    time.sleep(120)

                    # Recheck the internet connectivity after attempting to reconnect
                    if not check_internet_connectivity():
                        print("Reconnection failed. Skipping upload and continuing the loop...")
                        continue  # Skip to the next iteration of the loop

                # If the internet is working, proceed with the upload tasks
                logging.info("Internet is working.")

                # Upload image to S3 and GCP
                aws_uploaded, dataintegrityAWS, gcp_uploaded, dataintegrityGCP = upload_files_to_cloud(
                    image_path, image_filename, AWS_image_bucket_name, GCP_image_bucket_name, aws_upload, gcp_upload, s3_client, gcs_client)

                if aws_uploaded:
                    execute_db_operation(update_file_record_aws, cursor, image_filename, s3_image_path,
                                         AWS_image_bucket_name, dataintegrityAWS)
                if gcp_uploaded:
                    execute_db_operation(update_file_record_gcp, cursor, image_filename, gcs_image_path,
                                         GCP_image_bucket_name, dataintegrityGCP)

                # Upload Video to S3 and GCP
                aws_uploaded, dataintegrityAWS, gcp_uploaded, dataintegrityGCP = upload_files_to_cloud(
                    video_path, video_filename, AWS_video_bucket_name, GCP_video_bucket_name, aws_upload, gcp_upload, s3_client, gcs_client)

                if aws_uploaded:
                    execute_db_operation(update_file_record_aws, cursor, video_filename, s3_video_path,
                                         AWS_video_bucket_name, dataintegrityAWS)
                if gcp_uploaded:
                    execute_db_operation(update_file_record_gcp, cursor, video_filename, gcs_video_path,
                                         GCP_video_bucket_name, dataintegrityGCP)

                # Upload any unuploaded files
                upload_unuploaded_files(cursor, AWS_image_bucket_name, AWS_video_bucket_name, GCP_image_bucket_name,
                                        GCP_video_bucket_name, s3_client, gcs_client, aws_upload, gcp_upload)

                # Clean up old files at a specific time
                if now.hour == 23 and now.minute < 30:
                    delete_old_files(cursor, timezone=mst, days_old=30, aws_upload=aws_upload, gcp_upload=gcp_upload)

        except Exception as e:
            logging.error(f"An error occurred during the main loop: {e}")
            send_email('An error occurred during the main loop', f'An error occurred during the main loop: {e}')
            logging.info("Continuing with the next iteration despite the error.")

        logging.info('\n -------------Starting a new cycle ------------\n')


if __name__ == "__main__":
    main_loop()
