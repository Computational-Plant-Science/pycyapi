import ssl
from os.path import isdir, expanduser
from pathlib import Path
from typing import List

from irods.session import iRODSSession

from plantit_cli.collection.collection import Collection
from plantit_cli.collection.util import *


class IRODSOptions:
    def __init__(self, host, port, username, password, zone):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.zone = zone


class IRODSStore(Collection):

    @property
    def path(self):
        return self.__path

    def __init__(self, path: str, options: IRODSOptions = None):
        self.__path = path
        self.options = options

    def __session(self):
        if self.options is not None:
            for arg in [self.options.host, self.options.port, self.options.username, self.options.password, self.options.zone]:
                assert arg is not None
            return iRODSSession(host=self.options.host,
                                port=self.options.port,
                                user=self.options.username,
                                password=self.options.password,
                                zone=self.options.zone)
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

    def pull(self, to_path, pattern):
        raise NotImplementedError()
        Path(to_path).mkdir(parents=True, exist_ok=True)  # create local directory if it does not exist
        session = self.__session()

        is_remote_file = session.data_objects.exists(self.path)
        is_remote_dir = session.collections.exists(self.path)
        is_local_file = isfile(to_path)
        is_local_dir = isdir(to_path)

        if not (is_remote_dir or is_remote_file):
            raise FileNotFoundError(f"iRODS path '{self.path}' does not exist")
        elif not (is_local_dir or is_local_file):
            raise FileNotFoundError(f"Local path '{to_path}' does not exist")
        elif is_remote_dir and is_local_dir:
            for file in session.collections.get(self.path).data_objects:
                session.data_objects.get(file.path, join(to_path, file.path.split('/')[-1]))
        elif is_remote_file and is_local_file:
            session.data_objects.get(self.path, to_path)
        elif is_remote_file and is_local_dir:
            session.data_objects.get(self.path, join(to_path, self.path.split('/')[-1]))
        else:
            raise ValueError(
                f"Cannot overwrite local file '{to_path}' with contents of remote directory '{self.path}' (specify "
                f"a local directory instead)")

        session.cleanup()

    def push(self, from_path, pattern):
        raise NotImplementedError()
        session = self.__session()

        is_local_file = isfile(from_path)
        is_local_dir = isdir(from_path)

        if not (is_local_dir or is_local_file):
            raise FileNotFoundError(f"Local path '{from_path}' does not exist")
        elif is_local_dir:
            for file in list_files(from_path):
                session.data_objects.put(file, join(self.path, file.split('/')[-1]))
        elif is_local_file:
            session.data_objects.put(from_path, join(self.path, from_path.split('/')[-1]))
        else:
            raise ValueError(
                f"Cannot overwrite iRODS object '{self.path}' with contents of local directory '{from_path}' (specify a remote directory instead)")

        session.cleanup()
