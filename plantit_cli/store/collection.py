from abc import ABC, abstractmethod
from typing import List


class Collection(ABC):
    """
    A remote file/object store.
    """

    @abstractmethod
    def list(self) -> List[str]:
        """
        Lists the object(s) in the collection.

        Returns: The object(s) in the collection.

        """
        pass

    @abstractmethod
    def pull(self, path, pattern):
        """
        Pulls file(s) matching a pattern from the collection to the given path on the file system.

        Args:
            path: The path.
            pattern: The file pattern (e.g., extension).
        """
        pass

    @abstractmethod
    def push(self, path, pattern):
        """
        Pushes files(s) matching a pattern from the given path on the file system to the collection.

        Args:
            path: The local path.
            pattern: The file pattern (e.g., extension).
        """
        pass

