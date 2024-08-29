import os
from datetime import datetime
import cv2
import logging
from network import send_email


def capture_image(cap, image_base_directory, timezone):
    """
    Captures an image from the video capture device and saves it to the specified directory.

    Args:
    cap (cv2.VideoCapture): The video capture object (e.g., from a webcam or camera).
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
        # Read a frame from the video capture device
        ret, frame = cap.read()

        # Check if the frame was successfully captured
        if ret:
            # Ensure the base directory for images exists, create it if not
            os.makedirs(image_base_directory, exist_ok=True)

            # Generate a filename based on the current time in the specified timezone
            image_filename = f"image_capture_{datetime.now(timezone).strftime('%Y-%m-%d_%H-%M-%S')}.jpg"

            # Create the full image path by joining the base directory with the filename
            image_path = os.path.join(image_base_directory, image_filename)

            # Save the captured frame as an image file
            cv2.imwrite(image_path, frame)

            # Return the path and filename of the saved image
            return image_path, image_filename

        # If the capture failed, return None for both values
        return None, None

    except Exception as e:
        # Log the error for debugging and monitoring
        logging.error(f"Error capturing image: {e}")

        # Prepare and send an email notification about the error
        subject = 'Error capturing image'
        body = f"Error capturing image: {e}"
        send_email(subject, body)

        # Return None for both values in case of an error
        return None, None


def capture_video(cap, video_base_directory, timezone, frame_width, frame_height, fps, duration=40):
    """
    Captures a video from the video capture device and saves it to the specified directory.

    Args:
    cap (cv2.VideoCapture): The video capture object (e.g., from a webcam or camera).
    video_base_directory (str): The base directory where the captured video will be saved.
    timezone (datetime.tzinfo): The timezone information to timestamp the video filename correctly.
    frame_width (int): The width of the video frames.
    frame_height (int): The height of the video frames.
    fps (int): Frames per second for the video recording.
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

        # Create the full video path by joining the base directory with the filename
        video_path = os.path.join(video_base_directory, video_filename)

        # Define the video codec (using 'mp4v' codec for .mp4 format)
        video_codec = cv2.VideoWriter_fourcc(*'mp4v')

        # Initialize the VideoWriter object with the specified codec, FPS, and frame size
        video_output = cv2.VideoWriter(video_path, video_codec, fps, (frame_width, frame_height))

        # Calculate the total number of frames to capture based on FPS and duration
        total_frames = int(fps * duration)

        # Capture each frame and write it to the video file
        logging.info(f"Capturing video at {video_path}")
        for _ in range(total_frames):
            ret, frame = cap.read()  # Read a frame from the video capture device
            video_output.write(frame)  # Write the frame to the video file
        logging.info(f"Finished Capturing video at {video_path}")

        # Release the VideoWriter object to ensure the video file is properly saved
        video_output.release()

        # Return the path and filename of the saved video
        return video_path, video_filename

    except Exception as e:
        # Print and log the error for debugging and monitoring
        logging.error(f"Error capturing video: {e}")

        # Prepare and send an email notification about the error
        subject = 'Error capturing video'
        body = f"Error capturing video: {e}"
        send_email(subject, body)
        return None, None
