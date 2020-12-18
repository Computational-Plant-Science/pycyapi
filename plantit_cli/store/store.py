from abc import ABC, abstractmethod
from typing import List

from plantit_cli.plan import Plan


class Store(ABC):
    """
    Models a hierarchical file/object/blob store.
    """
    @abstractmethod
    def __init__(self, plan: Plan):
        self.__plan = plan

    @property
    def plan(self):
        return self.__plan

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
    def download_directory(self, from_path, to_path, patterns):
        """
        Downloads files from the remote directory to the local directory.

        Args:
            from_path: The remote directory path.
            to_path: The local directory path.
            patterns: File patterns to include.
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
    def upload_directory(self, from_path, to_path, include_pattern, include, exclude_pattern, exclude):
        """
        Uploads files from the local directory to the remote directory.

        Args:
            from_path: The local directory path.
            to_path: The remote directory path.
            include_pattern: File pattern to include.
            include: Files to include.
            exclude_pattern: File pattern to exclude.
            exclude: Files to exclude.
        """
        pass

