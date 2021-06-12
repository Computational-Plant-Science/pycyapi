from abc import ABC, abstractmethod
from typing import List


class Store(ABC):
    """
    Models a hierarchical file/object/blob store.
    """

    @abstractmethod
    def dir_exists(self, path) -> bool:
        """
        Determines whether a directory exists with the given path.

        Args:
            path: The directory path.

        Returns:
            True if the directory exists, otherwise False.
        """
        pass

    @abstractmethod
    def file_exists(self, path: str) -> bool:
        """
        Determines whether a file exists with the given path.

        Args:
            path: The file path.

        Returns:
            True if the file exists, otherwise False.
        """
        pass

    @abstractmethod
    def list_dir(self, path: str) -> List[str]:
        """
        Lists files in the given directory.

        Args:
            path: The directory path.

        Returns:
            A list of all files in the directory.
        """
        pass

    @abstractmethod
    def pull_file(self, from_path: str, to_path: str, overwrite: bool = False):
        """
        Copies a file from the store to a local directory.

        Args:
            from_path: The path (prefix followed by name) of the file in the store.
            to_path: The local directory path.
            overwrite: Whether to overwrite an already-existing file.
        """
        pass

    @abstractmethod
    def pull_dir(self, from_prefix: str, to_path: str, patterns: List[str], checksums: List[dict] = None, overwrite: bool = False):
        """
        Copies files under the given prefix from the store to the local directory path.

        Args:
            from_prefix: The prefix.
            to_path: The local directory path.
            patterns: File patterns to include.
            checksums: File checksums (if supported).
            overwrite: Whether to overwrite already-existing files.
        """
        pass

    @abstractmethod
    def push_file(self, from_path: str, to_prefix: str):
        """
        Adds the given file to the store with the given prefix.

        Args:
            from_path: The local file path.
            to_prefix: The prefix.
        """
        pass

    @abstractmethod
    def push_dir(
            self,
            from_path: str,
            to_prefix: str,
            include_pattern: List[str],
            include: List[str],
            exclude_pattern: List[str],
            exclude: List[str]):
        """
        Adds files from the local directory to the store under the given prefix.

        Args:
            from_path: The local directory path.
            to_prefix: The prefix.
            include_pattern: File patterns to include.
            include: Files to include.
            exclude_pattern: File patterns to exclude.
            exclude: Files to exclude.
        """
        pass
