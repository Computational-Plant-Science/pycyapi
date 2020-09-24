from typing import List
from os.path import isdir, isfile

import requests

from plantit_cli.store.collection import Collection
from plantit_cli.store.util import list_files


class TerrainStore(Collection):

    @property
    def path(self):
        return self.__path

    def __init__(self, path: str, token: str):
        self.__path = path
        self.__token = token

    def list(self) -> List[str]:
        response = requests.get(f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={self.__path}", headers={'Authorization': f"Bearer {self.__token}"})
        if response.status_code == 401:
            raise RuntimeError('CyVerse authentication cyverse_token expired or invalid')
        content = response.json()
        files = [file['path'] for file in content['files']]
        return files

    def pull(self, path, pattern):
        paths = self.list()
        for path in [p for p in paths if pattern in p]:
            with requests.get(f"https://de.cyverse.org/terrain/secured/fileio/download?path={path}", headers={'Authorization': f"Bearer {self.__token}"}) as response:
                if response.status_code == 401:
                    raise RuntimeError('CyVerse authentication cyverse_token expired or invalid')
                with open(f"{path}/{path.split('/')[-1]}", 'wb') as file:
                    for chunk in response.iter_content(chunk_size=8192):
                        # If you have chunk encoded response uncomment if
                        # and set chunk_size parameter to None.
                        # if chunk:
                        file.write(chunk)

    def push(self, path, pattern):
        is_local_file = isfile(path)
        is_local_dir = isdir(path)

        if not (is_local_dir or is_local_file):
            raise FileNotFoundError(f"Local path '{path}' does not exist")
        elif is_local_dir:
            paths = list_files(path)
            print(f"Preparing to upload {paths} files")
            for path in [p for p in paths if pattern in p]:
                print(f"Uploading {path} to {self.__path}")
                response = requests.post(f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={self.__path}", headers={'Authorization': f"Bearer {self.__token}"}, files={'file': open(path, 'rb')})
                if response.status_code == 401:
                    raise RuntimeError('CyVerse authentication cyverse_token expired or invalid')
                if response.status_code == 500:
                    raise RuntimeError(f'Internal server error')
        elif is_local_file:
            print(f"Uploading {path} to {self.__path}")
            response = requests.post(f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={self.__path}",
                                     headers={'Authorization': f"Bearer {self.__token}"},
                                     files={'file': open(path, 'rb')})
            if response.status_code == 401:
                raise RuntimeError('CyVerse authentication cyverse_token expired or invalid')
            if response.status_code == 500:
                raise RuntimeError(f'Internal server error')
        else:
            raise ValueError(
                f"Cannot overwrite iRODS object '{self.path}' with contents of local directory '{path}' (specify a remote directory instead)")