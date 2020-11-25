from abc import ABC, abstractmethod
from typing import List


class Store(ABC):
    """
    Models a hierarchical file/object/blob store.
    """

    @abstractmethod
    def list_directory(self, path) -> List[str]:
        """
        Lists files in the remote directory.

        Args:
            path: The remote directory path.

        Returns:
            A list of all files in the remote directory.
        """
        pass

    @abstractmethod
    def download_file(self, from_path, to_path):
        """
        Downloads the file at the remote path to the local path.

        Args:
            from_path: The remote path.
            to_path: The local path.
        """
        pass

    @abstractmethod
    def download_directory(self, from_path, to_path, pattern):
        """
        Downloads files from the remote directory to the local directory.

        Args:
            from_path: The remote directory path.
            to_path: The local directory path.
            pattern: File pattern(s) to include.
        """
        pass

    @abstractmethod
    def upload_file(self, from_path, to_path):
        """
        Uploads the file at the local path to the remote path.

        Args:
            from_path: The local file path.
            to_path: The remote path.
        """
        pass

    @abstractmethod
    def upload_directory(self, from_path, to_path, pattern, exclude):
        """
        Uploads files from the local directory to the remote directory.

        Args:
            from_path: The local directory path.
            to_path: The remote directory path.
            pattern: File pattern(s) to include.
            exclude: File pattern(s) to exclude.
        """
        pass

