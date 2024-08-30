# collector

The system captures images and videos, stores them locally, and uploads them to either AWS S3, Google Cloud Storage, or both. Additionally, old files are managed and removed from local storage, while the system can send alerts via email and reset network connections if necessary.

## Project Structure

- **`main.py`**: Main entry point of the project. Orchestrates the overall flow.
- **`Capture.py`**: Handles image and video capture from the camera.
- **`AWSbucketmanager.py`**: Manages file uploads and deletions in AWS S3 buckets.
- **`GCSbucketmanager.py`**: Manages file uploads and deletions in Google Cloud Storage.
- **`cloudupload.py`**: Manages the process of uploading data to cloud storage (AWS or GCP) and handles any unuploaded files.
- **`Storagemanager.py`**: Handles deletion of old files from local storage to free up space.
- **`databaseread.py`**: Performs read operations from the local SQLite database.
- **`databasewrite.py`**: Handles write operations and initializes the local SQLite database.
- **`network.py`**: Resets network connections and sends email notifications.
- **`utils.py`**: Provides utility functions such as calculating the next capture time, generating MD5 checksums, and checking data integrity.
- **`config.ini`**: Configuration file containing settings for cloud storage, email notifications, and other parameters.

## Installation

### Prerequisites

- Python 3.x
- A Raspberry Pi (or other system for capturing images and videos)

### Install Required Libraries

Run the following command to install the required Python libraries:

```bash
pip install -r requirements.txt

