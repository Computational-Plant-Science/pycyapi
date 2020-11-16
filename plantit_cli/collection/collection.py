from abc import ABC, abstractmethod
from typing import List


class Collection(ABC):
    """
    Models a folder or directory within a hierarchical file store.
    """

    @abstractmethod
    def list(self) -> List[str]:
        """
        Lists files in the collection.

        Returns:
            A list of all files in the collection.
        """
        pass

    @abstractmethod
    def pull(self, to_path, pattern):
        """
        Pulls files in the collection to the local path.

        Args:
            to_path: The local path.
            pattern: The file pattern.
        """
        pass

    @abstractmethod
    def push(self, from_path, pattern, exclude):
        """
        Pushes files from the local path to the collection.

        Args:
            from_path: The local path.
            pattern: File pattern(s) to include.
            exclude: File pattern(s) to exclude.
        """

        pass

