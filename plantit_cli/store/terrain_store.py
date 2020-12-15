from os import listdir
from typing import List
from os.path import isdir, isfile, basename, join

import requests

from plantit_cli.exceptions import PlantitException
from plantit_cli.plan import Plan
from plantit_cli.store.store import Store
from plantit_cli.utils import update_status, list_files


class TerrainStore(Store):
    def __init__(self, plan: Plan):
        super().__init__(plan)

    def list_directory(self, path) -> List[str]:
        with requests.get(
                f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={path}",
                headers={'Authorization': f"Bearer {self.plan.cyverse_token}"}) as response:
            if response.status_code == 500 and response.json()['error_code'] == 'ERR_DOES_NOT_EXIST':
                raise PlantitException(f"Path {path} does not exist")
            response.raise_for_status()
            content = response.json()
            return [file['path'] for file in content['files']]

    def download_file(self, from_path, to_path):
        to_path_full = f"{to_path}/{from_path.split('/')[-1]}"
        if isfile(to_path_full) and ('overwrite' not in self.plan.input or ('overwrite' in self.plan.input and not self.plan.input['overwrite'])):
            update_status(self.plan, 3, f"File {to_path_full} already exists, skipping download")
            return
        else:
            update_status(self.plan, 3, f"Downloading '{from_path}' to '{to_path_full}'")
        with requests.get(f"https://de.cyverse.org/terrain/secured/fileio/download?path={from_path}",
                          headers={'Authorization': f"Bearer {self.plan.cyverse_token}"}) as response:
            if response.status_code == 500 and response.json()['error_code'] == 'ERR_REQUEST_FAILED':
                raise PlantitException(f"Path {from_path} does not exist")
            response.raise_for_status()
            with open(to_path_full, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

    def download_directory(self, from_path, to_path, include_pattern=None):
        from_paths = [p for p in self.list_directory(from_path) if
                      include_pattern in p] if include_pattern is not None else self.list_directory(from_path)
        update_status(self.plan, 3, f"Downloading directory '{from_path}' with {len(from_paths)} file(s)")
        for path in from_paths:
            self.download_file(path, to_path)

    def upload_file(self, from_path, to_path):
        update_status(self.plan, 3, f"Uploading '{from_path}' to '{to_path}'")
        with open(from_path, 'rb') as file:
            with requests.post(f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={to_path}",
                               headers={'Authorization': f"Bearer {self.plan.cyverse_token}"},
                               files={'file': (basename(from_path), file, 'application/octet-stream')}) as response:
                response.raise_for_status()

    def upload_directory(self, from_path, to_path, include_pattern=None, include=None, exclude_pattern=None, exclude=None):
        is_file = isfile(from_path)
        is_dir = isdir(from_path)
        if not (is_dir or is_file):
            raise FileNotFoundError(f"Local path '{from_path}' does not exist")
        elif is_dir:
            from_paths = list_files(from_path, include_pattern, include, exclude_pattern, exclude)
            update_status(self.plan, 3, f"Uploading directory '{from_path}' with {len(from_paths)} files")
            for path in [str(p) for p in from_paths]:
                self.upload_file(path, to_path)
        elif is_file:
            self.upload_file(from_path, to_path)
        else:
            raise ValueError(
                f"Remote path '{to_path}' is a file; specify a directory path instead")
