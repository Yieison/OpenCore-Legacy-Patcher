"""
network_handler.py: Library dedicated to Network Handling tasks including downloading files

Primarily based around the DownloadObject class, which provides a simple
object for libraries to query download progress and status
"""

import time
import requests
import threading
import logging
import enum
import hashlib
import atexit

from http import HTTPStatus
from typing import BinaryIO, Optional, Union
from pathlib import Path

from . import utilities

SESSION = requests.Session()

DOWNLOAD_CHUNK_SIZE:       int = 1024 * 1024 * 4
DOWNLOAD_TIMEOUT:          int = 10
DOWNLOAD_MAX_RETRIES:      int = 5
DOWNLOAD_RETRY_BASE_DELAY: int = 2
DOWNLOAD_RETRY_MAX_DELAY:  int = 30

DOWNLOAD_RETRYABLE_STATUS_CODES: frozenset = frozenset({408, 429, 500, 502, 503, 504})
DOWNLOAD_RETRYABLE_ERRORS:       tuple = (requests.exceptions.ConnectionError, requests.exceptions.Timeout, requests.exceptions.ChunkedEncodingError)


class DownloadStatus(enum.Enum):
    """
    Enum for download status
    """

    INACTIVE:    str = "Inactive"
    DOWNLOADING: str = "Downloading"
    ERROR:       str = "Error"
    COMPLETE:    str = "Complete"


class NetworkUtilities:
    """
    Utilities for network related tasks, primarily used for downloading files
    """

    def __init__(self, url: str = None) -> None:
        self.url: str = url

        if self.url is None:
            self.url = "https://github.com"


    def verify_network_connection(self) -> bool:
        """
        Verifies that the network is available

        Returns:
            bool: True if network is available, False otherwise
        """

        try:
            requests.head(self.url, timeout=5, allow_redirects=True)
            return True
        except (
            requests.exceptions.Timeout,
            requests.exceptions.TooManyRedirects,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError
        ):
            return False

    def validate_link(self) -> bool:
        """
        Check for error

        Returns:
            bool: True if link is valid, False otherwise
        """
        try:
            response = SESSION.head(self.url, timeout=5, allow_redirects=True)
            response.raise_for_status()
            return True
        except (
            requests.exceptions.Timeout,
            requests.exceptions.TooManyRedirects,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError
        ):
            return False


    def get(self, url: str, **kwargs) -> requests.Response:
        """
        Wrapper for requests's get method
        Implement additional error handling

        Parameters:
            url (str): URL to get
            **kwargs: Additional parameters for requests.get

        Returns:
            requests.Response: Response object from requests.get
        """

        result: requests.Response = None

        try:
            result = SESSION.get(url, **kwargs)
        except (
            requests.exceptions.Timeout,
            requests.exceptions.TooManyRedirects,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError
        ) as error:
            logging.warning(f"Error calling requests.get: {error}")
            # Return empty response object
            return requests.Response()

        return result

    def post(self, url: str, **kwargs) -> requests.Response:
        """
        Wrapper for requests's post method
        Implement additional error handling

        Parameters:
            url (str): URL to post
            **kwargs: Additional parameters for requests.post

        Returns:
            requests.Response: Response object from requests.post
        """

        result: requests.Response = None

        try:
            result = SESSION.post(url, **kwargs)
        except (
            requests.exceptions.Timeout,
            requests.exceptions.TooManyRedirects,
            requests.exceptions.ConnectionError,
            requests.exceptions.HTTPError
        ) as error:
            logging.warning(f"Error calling requests.post: {error}")
            # Return empty response object
            return requests.Response()

        return result


class DownloadObject:
    """
    Object for downloading files from the network

    Usage:
        >>> download_object = DownloadObject(url, path)
        >>> download_object.download(display_progress=True)

        >>> if download_object.is_active():
        >>>     print(download_object.get_percent())

        >>> if not download_object.download_complete:
        >>>     print("Download failed")

        >>> print("Download complete"")

    """

    def __init__(self, url: str, path: str, checksum_algo: Optional["hashlib._Hash"] = None) -> None:
        self.url:       str = url
        self.status:    str = DownloadStatus.INACTIVE
        self.error_msg: str = ""
        self.filename:  str = self._get_filename()

        self.filepath:  Path = Path(path)

        self.total_file_size:      float = 0.0
        self.downloaded_file_size: float = 0.0
        self.start_time:           float = time.time()

        self.error:             bool = False
        self.download_complete: bool = False
        self.has_network:       bool = NetworkUtilities(self.url).verify_network_connection()
        self.retry_attempt:     int = 0

        self.active_thread: threading.Thread = None
        self._stop_event:   threading.Event = threading.Event()

        self.checksum = None
        self._checksum_storage: Optional[hashlib._Hash] = checksum_algo
        self._resume_validator: Optional[str] = None

        if self.has_network:
            self._populate_file_size()


    def __del__(self) -> None:
        self.stop()


    def download(self, display_progress: bool = False, spawn_thread: bool = True) -> None:
        """
        Download the file

        Spawns a thread to download the file, so that the main thread can continue
        Note sleep is disabled while the download is active
        Transient network errors are retried, resuming from the last byte received

        Parameters:
            display_progress (bool): Display progress in console
            spawn_thread (bool): Spawn a thread to download the file, otherwise download in the current thread
            verify_checksum (Optional[hashlib._Hash]): Checksum algorithm to use for verifying the download, optional

        """
        self.status = DownloadStatus.DOWNLOADING
        logging.info(f"Starting download: {self.filename}")
        if spawn_thread:
            if self.active_thread:
                logging.error("Download already in progress")
                return
            self.active_thread = threading.Thread(target=self._download, args=(display_progress,))
            self.active_thread.start()
            return

        self._download(display_progress)


    def download_simple(self, verify_checksum: bool = False) -> Union[str, bool]:
        """
        Alternative to download(), mimics  utilities.py's old download_file() function

        Parameters:
            verify_checksum (bool): Return checksum of downloaded file if True

        Returns:
            If verify_checksum is True, returns the checksum of the downloaded file
            Otherwise, returns True if download was successful, False otherwise
        """

        if verify_checksum:
            self._checksum_storage = hashlib.sha256()

        self.download(spawn_thread=False)

        if not self.download_complete:
            return False

        return self._checksum_storage.hexdigest() if self._checksum_storage else True


    def _get_filename(self) -> str:
        """
        Get the filename from the URL

        Returns:
            str: Filename
        """

        return Path(self.url).name


    def _populate_file_size(self) -> None:
        """
        Get the file size of the file to be downloaded

        If unable to get file size, set to zero
        """

        try:
            result = SESSION.head(self.url, allow_redirects=True, timeout=5)
            if 'Content-Length' in result.headers:
                self.total_file_size = float(result.headers['Content-Length'])
            else:
                raise Exception("Content-Length missing from headers")
        except Exception as e:
            logging.error(f"Error determining file size {self.url}: {str(e)}")
            logging.error("Assuming file size is 0")
            self.total_file_size = 0.0


    def _update_checksum(self, chunk: bytes) -> None:
        """
        Update checksum with new chunk

        Parameters:
            chunk (bytes): Chunk to update checksum with
        """
        if self._checksum_storage:
            self._checksum_storage.update(chunk)


    def _prepare_working_directory(self, path: Path) -> bool:
        """
        Validates working enviroment, including free space and removing existing files

        Parameters:
            path (str): Path to the file

        Returns:
            bool: True if successful, False if not
        """

        try:
            if Path(path).exists():
                logging.info(f"Deleting existing file: {path}")
                Path(path).unlink()

            if not Path(path).parent.exists():
                logging.info(f"Creating directory: {Path(path).parent}")
                Path(path).parent.mkdir(parents=True, exist_ok=True)

            available_space = utilities.get_free_space(Path(path).parent)
            if self.total_file_size > available_space:
                msg = f"Not enough free space to download {self.filename}, need {utilities.human_fmt(self.total_file_size)}, have {utilities.human_fmt(available_space)}"
                logging.error(msg)
                raise Exception(msg)

        except Exception as e:
            self.error = True
            self.error_msg = str(e)
            self.status = DownloadStatus.ERROR
            logging.error(f"Error preparing working directory {path}: {self.error_msg}")
            return False

        logging.info(f"- Directory ready: {path}")
        return True


    def _download(self, display_progress: bool = False) -> None:
        """
        Download the file

        Libraries should invoke download() instead of this method

        Parameters:
            display_progress (bool): Display progress in console
        """

        utilities.disable_sleep_while_running()

        try:
            if not self.has_network:
                raise Exception("No network connection")

            if self._prepare_working_directory(self.filepath) is False:
                raise Exception(self.error_msg)

            with open(self.filepath, 'wb') as file:
                atexit.register(self.stop)
                self._download_with_retries(file, display_progress)

            self.download_complete = True
            logging.info(f"Download complete: {self.filename}")
            logging.info("Stats:")
            logging.info(f"- Downloaded size: {utilities.human_fmt(self.downloaded_file_size)}")
            logging.info(f"- Time elapsed: {(time.time() - self.start_time):.2f} seconds")
            logging.info(f"- Speed: {utilities.human_fmt(self.downloaded_file_size / (time.time() - self.start_time))}/s")
            logging.info(f"- Location: {self.filepath}")
            if self._checksum_storage:
                self.checksum = self._checksum_storage.hexdigest()
                logging.info(f"Checksum: {self.checksum}")
            self.status = DownloadStatus.COMPLETE
        except Exception as e:
            self.error = True
            self.error_msg = str(e)
            self.status = DownloadStatus.ERROR
            logging.error(f"Error downloading {self.url}: {self.error_msg}")

        utilities.enable_sleep_after_running()


    def _download_with_retries(self, file: BinaryIO, display_progress: bool) -> None:
        """
        Write the file to disk, resuming from the last byte received after transient errors

        Gives up after DOWNLOAD_MAX_RETRIES consecutive attempts that don't get further than the previous ones

        Parameters:
            file (BinaryIO): File to write to
            display_progress (bool): Display progress in console
        """

        attempt = 0
        furthest_progress = 0.0

        while True:
            try:
                self._stream_to_file(file, display_progress)
                return
            except requests.exceptions.HTTPError as error:
                reason = f"Server responded with HTTP {error.response.status_code} ({error.response.reason})"
                if error.response.status_code not in DOWNLOAD_RETRYABLE_STATUS_CODES:
                    raise Exception(reason) from error
            except DOWNLOAD_RETRYABLE_ERRORS as error:
                reason = "Lost connection to the server"
                logging.warning(f"Download interrupted: {error}")

            if self.downloaded_file_size > furthest_progress:
                furthest_progress = self.downloaded_file_size
                attempt = 0
            attempt += 1

            if attempt > DOWNLOAD_MAX_RETRIES:
                raise Exception(f"{reason}, gave up after {DOWNLOAD_MAX_RETRIES} retries")

            delay = min(DOWNLOAD_RETRY_BASE_DELAY * 2 ** (attempt - 1), DOWNLOAD_RETRY_MAX_DELAY)
            logging.warning(f"{reason}, retrying in {delay} seconds ({attempt}/{DOWNLOAD_MAX_RETRIES})")
            self.retry_attempt = attempt
            if self._stop_event.wait(delay):
                raise Exception("Download stopped")


    def _stream_to_file(self, file: BinaryIO, display_progress: bool) -> None:
        """
        Request the file and write it to disk, resuming from the current file position

        Parameters:
            file (BinaryIO): File to write to
            display_progress (bool): Display progress in console
        """

        with self._request_from_position(file) as response:
            content_length = response.headers.get("Content-Length")
            expected_size = file.tell() + int(content_length) if content_length else 0
            if expected_size:
                self.total_file_size = float(expected_size)

            for i, chunk in enumerate(response.iter_content(DOWNLOAD_CHUNK_SIZE)):
                if self._stop_event.is_set():
                    raise Exception("Download stopped")
                if not chunk:
                    continue
                file.write(chunk)
                self._update_checksum(chunk)
                self.downloaded_file_size += len(chunk)
                self.retry_attempt = 0
                if display_progress and i % 100:
                    # Don't use logging here, as we'll be spamming the log file
                    if self.total_file_size == 0.0:
                        print(f"Downloaded {utilities.human_fmt(self.downloaded_file_size)} of {self.filename}")
                    else:
                        print(f"Downloaded {self.get_percent():.2f}% of {self.filename} ({utilities.human_fmt(self.get_speed())}/s) ({self.get_time_remaining():.2f} seconds remaining)")

        # urllib3 < 2.0 doesn't raise when the connection closes before Content-Length is reached
        if self.downloaded_file_size < expected_size:
            raise requests.exceptions.ConnectionError("Connection closed before the download finished")


    def _request_from_position(self, file: BinaryIO) -> requests.Response:
        """
        Request the file starting at the current file position

        Starts over if the server can't resume the download, or if the file changed on the server since it started

        Parameters:
            file (BinaryIO): File to write to

        Returns:
            requests.Response: Streamed response, its content continues at the current file position
        """

        offset = file.tell()
        # Byte offsets must match the bytes written to disk, so compression is disabled
        headers = {"Accept-Encoding": "identity"}
        if offset:
            headers["Range"] = f"bytes={offset}-"

        response = SESSION.get(self.url, headers=headers, stream=True, timeout=DOWNLOAD_TIMEOUT)

        if offset:
            # GitHub and Apple's CDN ignore If-Range, so the file version is verified here instead
            if response.status_code == HTTPStatus.PARTIAL_CONTENT and self._get_resume_validator(response) == self._resume_validator:
                return response
            if response.ok or response.status_code == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE:
                logging.warning("Unable to resume the download, restarting from the beginning")
                response.close()
                self._reset_progress(file)
                return self._request_from_position(file)

        if not response.ok:
            response.close()
            response.raise_for_status()

        self._resume_validator = self._get_resume_validator(response)
        return response


    def _reset_progress(self, file: BinaryIO) -> None:
        """
        Discard the data received so far

        Parameters:
            file (BinaryIO): File to write to
        """

        file.seek(0)
        file.truncate()
        self.downloaded_file_size = 0.0
        if self._checksum_storage:
            self._checksum_storage = hashlib.new(self._checksum_storage.name)


    @staticmethod
    def _get_resume_validator(response: requests.Response) -> Optional[str]:
        """
        Get the value identifying the version of the file, so a download is never resumed on a different version

        Weak ETags don't guarantee byte-identical content, so Last-Modified is used instead of them

        Parameters:
            response (requests.Response): Response to get the validator from

        Returns:
            str: ETag or Last-Modified value, None if the server provided neither
        """

        etag = response.headers.get("ETag")
        if etag and not etag.startswith("W/"):
            return etag
        return response.headers.get("Last-Modified")


    def get_percent(self) -> float:
        """
        Query the download percent

        Returns:
            float: The download percent, or -1 if unknown
        """

        if self.total_file_size == 0.0:
            return -1
        return self.downloaded_file_size / self.total_file_size * 100


    def get_speed(self) -> float:
        """
        Query the download speed

        Returns:
            float: The download speed in bytes per second
        """

        return self.downloaded_file_size / (time.time() - self.start_time)


    def get_time_remaining(self) -> float:
        """
        Query the time remaining for the download

        Returns:
            float: The time remaining in seconds, or -1 if unknown
        """

        if self.total_file_size == 0.0:
            return -1
        speed = self.get_speed()
        if speed <= 0:
            return -1
        return (self.total_file_size - self.downloaded_file_size) / speed


    def get_file_size(self) -> float:
        """
        Query the file size of the file to be downloaded

        Returns:
            float: The file size in bytes, or 0.0 if unknown
        """

        return self.total_file_size


    def is_active(self) -> bool:
        """
        Query if the download is active

        Returns:
            boolean: True if active, False if completed, failed, stopped, or inactive
        """

        if self.status == DownloadStatus.DOWNLOADING:
            return True
        return False


    def stop(self) -> None:
        """
        Stop the download

        If the download is active, this function will hold the thread until stopped
        """

        self._stop_event.set()
        if self.active_thread:
            while self.active_thread.is_alive():
                time.sleep(1)