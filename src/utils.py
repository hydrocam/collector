import logging
from datetime import datetime, timedelta
import hashlib


def extract_datetime_from_filename(filename):
    """
    Extracts the datetime, year, and month from the filename.

    Args:
    filename (str): The name of the file, expected to contain a datetime in the format
                    "prefix_YYYY-MM-DD_HH-MM-SS.suffix" (e.g., "image_capture_2024-08-09_14-30-00.jpg").

    Returns:
    tuple: A tuple containing:
           - The extracted datetime as a string in the format 'YYYY-MM-DD HH:MM:SS'.
           - The extracted year as a string (e.g., '2024').
           - The extracted month as a string (e.g., 'August').
           If extraction fails, returns (None, None, None).

    The function assumes that the filename contains a datetime in the format "YYYY-MM-DD_HH-MM-SS".
    It extracts the date and time parts, combines them, and converts them into a datetime object.
    The year and month are then extracted from this object.

    If an error occurs during extraction, the function logs the error and returns None values.
    """
    try:
        # Split the filename to extract the date and time parts
        date_str = filename.split('_')[2]  # Extract date part (e.g., "2024-08-09")
        time_str = filename.split('_')[3].split('.')[0]  # Extract time part (e.g., "14-30-00")

        # Combine date and time parts, and replace dashes in time with colons
        datetime_str = f"{date_str} {time_str.replace('-', ':')}"

        # Convert the combined string into a datetime object
        datetime_obj = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')

        # Extract year and month from the datetime object
        year = datetime_obj.strftime('%Y')  # Extract year (e.g., "2024")
        month = datetime_obj.strftime('%B')  # Extract full month name (e.g., "August")

        # Return the formatted datetime string, year, and month
        return datetime_obj.strftime('%Y-%m-%d %H:%M:%S'), year, month

    except Exception as e:
        # Log the error and return None values in case of an exception
        logging.error(f'Error extracting datetime from filename: {e}')
        return None, None, None


def get_next_capture_time(mst):
    """
    Calculates the next capture time rounded to the next 30-minute interval.

    Args:
    mst (datetime.tzinfo): The timezone information for the current datetime.

    Returns:
    datetime: The next capture time, rounded up to the nearest 30-minute interval.

    This function calculates the next capture time based on the current time in the given timezone.
    It rounds the next capture time up to the nearest 30-minute interval. If the current time is exactly
    at the start of a 30-minute interval, it will correctly find the next interval.

    Example:
    If the current time is 2024-08-26 14:23:45, the next capture time will be 2024-08-26 14:30:00.
    If the current time is 2024-08-26 14:30:00, the next capture time will be 2024-08-26 15:00:00.
    """
    # Get the current time in the specified timezone
    now = datetime.now(mst)

    # Calculate the next 60-minute interval
    minutes = 60
    next_capture = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=minutes)

    # If the calculated time is earlier than or equal to the current time (in case now is exactly at the start of a 30-minute interval)
    if next_capture <= now:
        next_capture += timedelta(minutes=30)

    return next_capture


def calculate_md5(file_path):
    """
    Calculates the MD5 checksum of a file.

    Args:
    file_path (str): The path to the file for which the MD5 checksum is to be calculated.

    Returns:
    str: The MD5 checksum of the file as a hexadecimal string.

    This function opens the specified file in binary mode and reads it in chunks of 4096 bytes.
    It updates the MD5 hash object with each chunk and then returns the hexadecimal digest of the hash.

    Example:
    >>> calculate_md5('/path/to/file.txt')
    'd41d8cd98f00b204e9800998ecf8427e'
    """
    # Create an MD5 hash object
    md5_hash = hashlib.md5()

    # Open the file in binary mode
    with open(file_path, "rb") as f:
        # Read the file in chunks of 4096 bytes
        for byte_block in iter(lambda: f.read(4096), b""):
            # Update the MD5 hash object with the bytes read
            md5_hash.update(byte_block)

    # Return the hexadecimal digest of the MD5 hash
    return md5_hash.hexdigest()


def data_integrity_check(file_path, cloud_md5):
    """
    Verifies the integrity of a file by comparing its local MD5 checksum with a provided cloud MD5 checksum.

    Args:
    file_path (str): The path to the local file for which the MD5 checksum is to be calculated.
    cloud_md5 (str): The MD5 checksum of the file as provided by the cloud storage service.

    Returns:
    bool: True if the local MD5 checksum matches the cloud MD5 checksum, False otherwise.

    This function calculates the MD5 checksum of the specified local file and compares it with the provided
    cloud MD5 checksum. It logs an info message if the checksums match, indicating successful verification.
    If the checksums do not match, it logs an error message indicating a verification failure.

    Example:
    >>> data_integrity_check('/path/to/file.txt', 'd41d8cd98f00b204e9800998ecf8427e')
    True
    """
    try:
        # Calculate the MD5 checksum of the local file
        local_md5 = calculate_md5(file_path)

        # Compare the local MD5 checksum with the cloud MD5 checksum
        if local_md5 == cloud_md5:
            logging.info("Upload verified successfully! MD5 checksums match.")
            return True
        else:
            logging.error("Upload verification failed! MD5 checksums do not match.")
            return False
    except Exception as e:
        # Log an error if there is an issue calculating the MD5 checksum
        logging.error(f"Error during data integrity check: {e}")
        return False
