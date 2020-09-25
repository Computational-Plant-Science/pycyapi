from typing import List
from os.path import isdir, isfile

import requests

from plantit_cli.collection.collection import Collection
from plantit_cli.collection.util import list_files


class TerrainStore(Collection):

    @property
    def path(self):
        return self.__path

    def __init__(self, path: str, token: str):
        self.__path = path
        self.__token = token

    def list(self) -> List[str]:
        response = requests.get(
            f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={self.__path}",
            headers={'Authorization': f"Bearer {self.__token}"})
        if response.status_code == 401:
            raise RuntimeError('CyVerse authentication cyverse_token expired or invalid')
        content = response.json()
        files = [file['path'] for file in content['files']]
        return files

    def pull(self, to_path, pattern=None):
        from_paths = [p for p in self.list() if pattern in p] if pattern is not None else self.list()
        print(f"Preparing to pull {len(from_paths)} files")
        for from_path in from_paths:
            print(f"Pulling '{from_path}' to '{to_path}'")
            with requests.get(f"https://de.cyverse.org/terrain/secured/fileio/download?path={from_path}",
                              headers={'Authorization': f"Bearer {self.__token}"}) as response:
                if response.status_code == 401:
                    raise RuntimeError('CyVerse authentication cyverse_token expired or invalid')
                # with open(f"{from_path}/{from_path.split('/')[-1]}", 'wb') as file:
                with open(f"{to_path}/{from_path.split('.')[-1]}", 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        # If you have chunk encoded response uncomment if
                        # and set chunk_size parameter to None.
                        # if chunk:
                        file.write(chunk)

    def push(self, from_path, pattern=None):
        is_local_file = isfile(from_path)
        is_local_dir = isdir(from_path)
        if not (is_local_dir or is_local_file):
            raise FileNotFoundError(f"Local path '{from_path}' does not exist")
        elif is_local_dir:
            from_paths = [p for p in list_files(from_path) if pattern in p] if pattern is not None else list_files(from_path)
            print(f"Preparing to push {len(from_paths)} files")
            for from_path in [p for p in from_paths if pattern in p]:
                print(f"Pushing '{from_path}' to '{self.__path}'")
                response = requests.post(f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={self.__path}",
                                         headers={'Authorization': f"Bearer {self.__token}"},
                                         files={'file': open(from_path, 'rb')})
                if response.status_code == 401:
                    raise RuntimeError('CyVerse authentication cyverse_token expired or invalid')
                if response.status_code == 500:
                    raise RuntimeError(f'Internal server error')
        elif is_local_file:
            print(f"Uploading {from_path} to {self.__path}")
            response = requests.post(f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={self.__path}",
                                     headers={'Authorization': f"Bearer {self.__token}"},
                                     files={'file': open(from_path, 'rb')})
            if response.status_code == 401:
                raise RuntimeError('CyVerse authentication cyverse_token expired or invalid')
            if response.status_code == 500:
                raise RuntimeError(f'Internal server error')
        else:
            raise ValueError(
                f"Cannot overwrite iRODS object '{self.path}' with contents of local directory '{from_path}' (specify a remote directory instead)")
