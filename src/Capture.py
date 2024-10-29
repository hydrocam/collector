import os
from datetime import datetime
import logging
from network import send_email
import ffmpeg


def capture_image(rtsp_url, image_base_directory, timezone):
    """
    Captures an image from the RTSP stream and saves it to the specified directory.

    Args:
    rtsp_url (str): The RTSP stream URL.
    image_base_directory (str): The base directory where the captured images will be saved.
    timezone (datetime.tzinfo): The timezone information to timestamp the image filename correctly.

    Returns:
    tuple:
        - image_path (str): The full path of the saved image.
        - image_filename (str): The name of the saved image file.

    If an image capture fails, the function returns (None, None).
    If an exception occurs during capture or saving, an email notification is sent, and (None, None) is returned.
    """
    try:
        # Ensure the base directory for images exists, create it if not
        os.makedirs(image_base_directory, exist_ok=True)

        # Generate a filename based on the current time in the specified timezone
        image_filename = f"image_capture_{datetime.now(timezone).strftime('%Y-%m-%d_%H-%M-%S')}.jpg"
        image_path = os.path.join(image_base_directory, image_filename)

        # Capture a single frame from the RTSP stream and save it as an image
        ffmpeg.input(rtsp_url, rtsp_transport='tcp').output(image_path, vframes=1, qscale=2).run()
        # Return the path and filename of the saved image
        return image_path, image_filename

    except Exception as e:
        logging.error(f"Error capturing image: {e}")
        send_email('Error capturing image', f"Error capturing image: {e}")
        return None, None


def capture_video(rtsp_url, video_base_directory, timezone, duration=40):
    """
    Captures a video from the RTSP stream and saves it to the specified directory.

    Args:
    rtsp_url (str): The RTSP stream URL.
    video_base_directory (str): The base directory where the captured video will be saved.
    timezone (datetime.tzinfo): The timezone information to timestamp the video filename correctly.
    duration (int, optional): The duration of the video in seconds (default is 40 seconds).

    Returns:
    tuple:
        - video_path (str): The full path of the saved video.
        - video_filename (str): The name of the saved video file.

    If a video capture fails or an error occurs, the function returns (None, None).
    If an exception occurs, an email notification is sent, and (None, None) is returned.
    """
    try:
        # Ensure the base directory for videos exists, create it if not
        os.makedirs(video_base_directory, exist_ok=True)

        # Generate a filename based on the current time in the specified timezone
        video_filename = f"video_capture_{datetime.now(timezone).strftime('%Y-%m-%d_%H-%M-%S')}.mp4"
        video_path = os.path.join(video_base_directory, video_filename)

        # Capture video from the RTSP stream for the specified duration
        ffmpeg.input(rtsp_url, rtsp_transport='tcp').output(video_path, vcodec='copy', t=duration).run()

        # Return the path and filename of the saved video
        return video_path, video_filename

    except Exception as e:
        logging.error(f"Error capturing video: {e}")
        send_email('Error capturing video', f"Error capturing video: {e}")
        return None, None