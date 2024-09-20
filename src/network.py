import logging
import requests
import subprocess
import boto3
import configparser
from botocore.exceptions import ClientError

def check_internet_connectivity(url='http://www.google.com', timeout=10):
    """
    Checks internet connectivity by sending a request to a specified URL.

    Args:
    url (str): The URL to send the request to. Defaults to 'http://www.google.com'.
    timeout (int): The timeout duration for the request in seconds. Defaults to 10 seconds.

    Returns:
    bool: True if the request is successful and the status code is 200, False otherwise.

    This function attempts to send a GET request to the specified URL. If the request is successful and
    the response status code is 200 (OK), it returns True, indicating that internet connectivity is present.
    If there is any issue with the request (e.g., timeout or connection error), it returns False.

    Example:
    >>> check_internet_connectivity()
    True
    >>> check_internet_connectivity(url='http://nonexistentwebsite.com')
    False
    """
    try:
        # Send a GET request to the specified URL with the given timeout
        response = requests.get(url, timeout=timeout)

        # Return True if the response status code is 200, indicating successful connectivity
        return response.status_code == 200
    except requests.RequestException:
        # Return False if there is any issue with the request
        return False


def disconnect_current_wifi():
    """
    Disconnects from the current Wi-Fi network by running the appropriate system command.

    This function uses the `wpa_cli` command-line tool to disconnect from the currently connected Wi-Fi network
    on the `wlan0` interface. It requires elevated privileges (sudo) to execute the command.

    Returns:
    None

    This function does not return any value. It logs success or error messages based on the outcome of the command execution.

    Example:
    >>> disconnect_current_wifi()
    Disconnected from the current Wi-Fi.
    """
    try:
        # Run the command to disconnect from the current Wi-Fi network
        subprocess.run(['sudo', 'wpa_cli', '-i', 'wlan0', 'disconnect'], check=True)
        logging.info("Disconnected from the current Wi-Fi.")
    except subprocess.CalledProcessError as e:
        # Log an error if the command fails
        logging.error(f"Error disconnecting current Wi-Fi: {e}")


def send_email(subject, body):
    """
    Sends an email using AWS SES.

    Args:
    subject (str): The subject of the email.
    body (str): The body of the email.

    Returns:
    None

    This function uses AWS Simple Email Service (SES) to send an email. The configuration details
    (AWS credentials and email addresses) are read from a 'config.ini' file.
    """
    if not check_internet_connectivity():
        print("No Internet connectivity.")
        return
        
    config = configparser.ConfigParser()
    config.read('config.ini')

    ses_client = boto3.client('ses', region_name='us-west-1',
                              aws_access_key_id=config['aws']['aws_access_key_id'],
                              aws_secret_access_key=config['aws']['aws_secret_access_key'])
    to_email = config['email']['receiver']

    try:
        response = ses_client.send_email(
            Source=config['email']['source'],  # Replace with your verified email
            Destination={
                'ToAddresses': [to_email]
            },
            Message={
                'Subject': {
                    'Data': subject
                },
                'Body': {
                    'Text': {
                        'Data': body
                    }
                }
            }
        )
        logging.info(f"Email sent! Message ID: {response['MessageId']}")

    except ClientError as e:
        logging.error(f"Error sending email: {e.response['Error']['Message']}")
