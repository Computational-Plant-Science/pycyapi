import ssl
from os.path import isdir, expanduser
from pathlib import Path
from typing import List

from irods.session import iRODSSession

from plantit_cli.store.store import Store
from plantit_cli.store.util import *


class IRODSOptions:
    def __init__(self, host, port, username, password, zone):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.zone = zone


class IRODSStore(Store):

    @property
    def path(self):
        return self.__path

    def __init__(self, path: str, options: IRODSOptions = None):
        self.__path = path
        if options is not None:
            self.host = options.host
            self.port = options.port
            self.username = options.username
            self.password = options.password
            self.zone = options.zone

    def __session(self):
        if self.host is not None:
            for arg in [self.port, self.username, self.password, self.zone]:
                assert arg is not None
            return iRODSSession(host=self.host,
                                port=self.port,
                                user=self.username,
                                password=self.password,
                                zone=self.zone)
        else:
            env_file = expanduser('~/.irods/irods_environment.json')
            ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH,
                                                     cafile=None,
                                                     capath=None,
                                                     cadata=None)
            return iRODSSession(irods_env_file=env_file, **{'ssl_context': ssl_context})

    def list(self) -> List[str]:
        session = self.__session()

        print(self.path)
        if session.collections.exists(self.path):
            collection = session.collections.get(self.path)
            files = [file.name for file in collection.data_objects]
            # directories = [directory.name for directory in collection.subcollections]
            return files  # + directories
        else:
            raise FileNotFoundError(f"iRODS path '{self.path}' does not exist")

    def pull(self, local_path):
        Path(local_path).mkdir(parents=True, exist_ok=True)  # create local directory if it does not exist
        session = self.__session()

        is_remote_file = session.data_objects.exists(self.path)
        is_remote_dir = session.collections.exists(self.path)
        is_local_file = isfile(local_path)
        is_local_dir = isdir(local_path)

        if not (is_remote_dir or is_remote_file):
            raise FileNotFoundError(f"iRODS path '{self.path}' does not exist")
        elif not (is_local_dir or is_local_file):
            raise FileNotFoundError(f"Local path '{local_path}' does not exist")
        elif is_remote_dir and is_local_dir:
            for file in session.collections.get(self.path).data_objects:
                session.data_objects.get(file.path, join(local_path, file.path.split('/')[-1]))
        elif is_remote_file and is_local_file:
            session.data_objects.get(self.path, local_path)
        elif is_remote_file and is_local_dir:
            session.data_objects.get(self.path, join(local_path, self.path.split('/')[-1]))
        else:
            raise ValueError(
                f"Cannot overwrite local file '{local_path}' with contents of remote directory '{self.path}' (specify "
                f"a local directory instead)")

        session.cleanup()

    def push(self, local_path):
        session = self.__session()

        is_local_file = isfile(local_path)
        is_local_dir = isdir(local_path)

        if not (is_local_dir or is_local_file):
            raise FileNotFoundError(f"Local path '{local_path}' does not exist")
        elif is_local_dir:
            for file in list_files(local_path):
                session.data_objects.put(file, join(self.path, file.split('/')[-1]))
        elif is_local_file:
            session.data_objects.put(local_path, join(self.path, local_path.split('/')[-1]))
        else:
            raise ValueError(
                f"Cannot overwrite iRODS object '{self.path}' with contents of local directory '{local_path}' (specify a remote directory instead)")

        session.cleanup()
