from abc import ABC, abstractmethod
from typing import List


class Input(ABC):
    """
    A remote file/object store.
    """

    @property
    @abstractmethod
    def path(self):
        pass

    @property
    @abstractmethod
    def param(self):
        pass

    @abstractmethod
    def list(self) -> List[str]:
        """
        Lists the object(s) in the collection.

        Returns: The object(s) in the collection.

        """
        pass

    @abstractmethod
    def pull(self, local_path):
        """
        Pulls the object(s) in the collection to the given location on the local file system.

        Args:
            local_path: The local path.
        """
        pass

    @abstractmethod
    def push(self, local_path):
        """
        Pushes the objects(s) at the given location on the local file system to the collection.

        Args:
            local_path: The local path.
        """
        pass
