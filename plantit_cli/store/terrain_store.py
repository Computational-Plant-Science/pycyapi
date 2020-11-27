import json
from typing import List
from os.path import isdir, isfile, basename

import requests

from plantit_cli.store.store import Store
from plantit_cli.store.util import list_files


class TerrainStore(Store):
    def __init__(self, token: str):
        self.__token = token

    def list_directory(self, path) -> List[str]:
        print(f"Listing '{path}'")
        with requests.get(
                f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={path}",
                headers={'Authorization': f"Bearer {self.__token}"}) as response:
            response.raise_for_status()
            content = response.json()
            return [file['path'] for file in content['files']]

    def download_file(self, from_path, to_path):
        to_path_full = f"{to_path}/{from_path.split('/')[-1]}"
        print(f"Downloading '{from_path}' to '{to_path_full}'")
        with requests.get(f"https://de.cyverse.org/terrain/secured/fileio/download?path={from_path}",
                          headers={'Authorization': f"Bearer {self.__token}"}) as response:
            response.raise_for_status()
            with open(to_path_full, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

    def download_directory(self, from_path, to_path, pattern=None):
        from_paths = [p for p in self.list_directory(from_path) if
                      pattern in p] if pattern is not None else self.list_directory(from_path)
        print(f"Downloading directory '{from_path}' with {len(from_paths)} file(s)")
        for path in from_paths:
            self.download_file(path, to_path)

    def upload_file(self, from_path, to_path):
        print(f"Uploading '{from_path}' to '{to_path}'")
        with open(from_path, 'rb') as file:
            with requests.post(f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={to_path}",
                               headers={'Authorization': f"Bearer {self.__token}"},
                               files={'file': (basename(from_path), file, 'application/octet-stream')}) as response:
                response.raise_for_status()

    def upload_directory(self, from_path, to_path, pattern=None, exclude=None):
        is_file = isfile(from_path)
        is_dir = isdir(from_path)

        if not (is_dir or is_file):
            raise FileNotFoundError(f"Local path '{from_path}' does not exist")
        elif is_dir:
            from_paths = list_files(from_path, pattern, exclude)
            print(f"Uploading directory '{from_path}' with {len(from_paths)} files")
            for path in [str(p) for p in from_paths]:
                self.upload_file(path, to_path)
        elif is_file:
            self.upload_file(from_path, to_path)
        else:
            raise ValueError(
                f"Remote path '{to_path}' is a file; specify a directory path instead")
