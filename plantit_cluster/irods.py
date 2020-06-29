import os
from pathlib import Path
from typing import List

from irods.session import iRODSSession


class IRODSOptions:
    def __init__(self, host: str, port: int, user: str, password: str, zone: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.zone = zone


class IRODS:
    def __init__(self, options: IRODSOptions):
        self.options = options

    def list(self, remote) -> List[str]:
        """
        Lists the files and directories at the given path on the remote IRODS instance.

        Args:
            remote: The remote path.

        Returns: The file/directory names.

        """
        remote = remote.rstrip("/")
        session = iRODSSession(host=self.options.host,
                               port=self.options.port,
                               user=self.options.user,
                               password=self.options.password,
                               zone=self.options.zone)

        if session.collections.exists(remote):
            collection = session.collections.get(remote)
            files = [file.name for file in collection.data_objects]
            directories = [directory.name for directory in collection.subcollections]
            return files + directories
        else:
            raise FileNotFoundError(f"Remote path '{remote}' does not exist")

    def get(self, remote, local):
        """
        Transfers the file or directory contents from the remote IRODS instance to the local file system.

        Args:
            remote: The remote file or directory path.
            local: The local directory path.
        """
        remote = remote.rstrip("/")
        Path(local).mkdir(parents=True, exist_ok=True)  # create local directory if it does not exist
        session = iRODSSession(host=self.options.host,
                               port=self.options.port,
                               user=self.options.user,
                               password=self.options.password,
                               zone=self.options.zone)

        if session.data_objects.exists(remote):
            session.data_objects.get(remote, file=os.path.join(local, os.path.basename(remote)))
        elif session.collections.exists(remote):
            collection = session.collections.get(remote)

            for file in collection.data_objects:
                self.get(os.path.join(remote, file.path), local)

            for sub_collection in collection.subcollections:
                nested = os.path.join(local, sub_collection.path.split("/")[-1])
                Path(nested).mkdir(exist_ok=True)
                self.get(os.path.join(remote, sub_collection.path), nested)
        else:
            raise FileNotFoundError(f"Remote path '{remote}' does not exist")

        session.cleanup()

    def put(self, local, remote):
        """
        Transfers the file or directory contents from the local file system to the remote IRODS instance.

        Args:
            local: The local file or directory path.
            remote: The remote directory path.
        """
        remote = remote.rstrip("/")
        session = iRODSSession(host=self.options.host,
                               port=self.options.port,
                               user=self.options.user,
                               password=self.options.password,
                               zone=self.options.zone)

        session.data_objects.put(local, remote)
        session.cleanup()



