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

### 1. Set up Virtual Environment

To create and activate a virtual environment for the project, follow these steps:
Step 1: Navigate to the Project Directory
Open your terminal and change to the project directory:
```bash
cd /path/to/folder/for/venv
```
Step 2: Create a Virtual Environment
Run the following command to create a virtual environment named collectorenv:
```bash
python3 -m venv collectorenv
```
Step 3: Activate the Virtual Environment
Activate the virtual environment using the following command:
```bash
source collectorenv/bin/activate
```

### 2. Clone the repository:

```bash
git clone https://github.com/hydrocam/collector
cd collector
```

### 3. Install Required Libraries

Run the following command to install the required Python libraries:

```bash
pip install -r requirements.txt
```

### 4. Prerequisites

- Python 3.x
- A Raspberry Pi (or other system for capturing images and videos)

## Configuration

You can configure various parameters (e.g.cloud storage preferences, camerea address) by editing the configuration file (config.ini).

### Running the Script at Reboot on Raspberry Pi

To ensure that the start_script.sh runs automatically every time the Raspberry Pi reboots, follow these steps:
#### 1. Edit the Root User's Crontab
Open the terminal and execute the following command:
```bash
sudo crontab -e
```
You might get an option like this when running this command for first time:
![image](https://github.com/user-attachments/assets/ce9691ea-9ffe-4a04-8d67-e2a0c6dc34a7)

Select 1 (nano) as our terminal editor.
#### 2. Add the Script to Run at Reboot
In the crontab editor, add the following line at last to schedule your script to run at every reboot:
```bash
@reboot bash /path/to/cloned/repo/folder/collector/start_script.sh
```
##### Note:
Replace /path/to/cloned/repo/ with the actual path where your repository is cloned.

## Funding and Acknowledgments

Funding for this project was provided by the National Oceanic & Atmospheric Administration (NOAA), awarded to the Cooperative Institute for Research to Operations in Hydrology (CIROH) through the NOAA Cooperative Agreement with The University of Alabama (NA22NWS4320003). Utah State University is a founding member of CIROH and receives funding under subaward from the University of Alabama. Additional funding and support have been provided by the Utah Water Research laboratory at Utah State University.
