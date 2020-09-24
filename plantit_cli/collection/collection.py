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
    def pull(self, to_path, pattern):
        """
        Pulls file(s) matching a pattern from the collection to the given path on the file system.

        Args:
            to_path: The file system path.
            pattern: The file pattern (e.g., extension).
        """
        pass

    @abstractmethod
    def push(self, from_path, pattern):
        """
        Pushes files(s) matching a pattern from the given path on the file system to the collection.

        Args:
            from_path: The file system path.
            pattern: The file pattern (e.g., extension).
        """
        pass

