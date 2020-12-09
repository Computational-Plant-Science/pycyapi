from os.path import isfile, isdir, join
from pathlib import Path
from shutil import copyfileobj
from typing import List

from plantit_cli.store.store import Store
from plantit_cli.store.util import list_files


class LocalStore(Store):
    def __init__(self, temp_dir):
        self.__files = {}
        self.__dir = temp_dir

    @property
    def dir(self):
        return self.__dir

    def download_file(self, from_path, to_path):
        from_path_file = join(self.__dir, from_path)
        to_path_file = join(to_path, from_path_file.split('/')[-1])
        with open(from_path_file, 'rb') as from_file, open(to_path_file, 'wb+') as to_file:
            print(f"Copying {from_path_file} to {to_path_file}")
            copyfileobj(from_file, to_file)

    def download_directory(self, from_path, to_path, pattern):
        from_paths = [p for p in self.list_directory(from_path) if
                      pattern.lower() in p.lower()] if pattern is not None else self.list_directory(from_path)
        for path in from_paths:
            self.download_file(path, to_path)

    def upload_file(self, from_path, to_path):
        to_path_dir = join(self.__dir, to_path)
        to_path_file = join(self.__dir, to_path, from_path.split('/')[-1])
        Path(to_path_dir).mkdir(parents=True, exist_ok=True)
        self.__files[to_path_file] = from_path
        with open(from_path, 'rb') as from_file, open(to_path_file, 'wb+') as to_file:
            print(f"Copying {from_path} to {to_path_file}")
            copyfileobj(from_file, to_file)

    def upload_directory(self, from_path, to_path, pattern, exclude):
        is_file = isfile(from_path)
        is_dir = isdir(from_path)

        if not (is_dir or is_file):
            raise FileNotFoundError(f"Path '{from_path}' does not exist")
        elif is_dir:
            from_paths = list_files(from_path, pattern, exclude)
            for path in [str(p) for p in from_paths]:
                self.upload_file(path, to_path)
        elif is_file:
            self.upload_file(from_path, to_path)
        else:
            raise ValueError(
                f"Path '{to_path}' is a file; specify a directory path instead")

    def list_directory(self, path) -> List[str]:
        for k, v in self.__files.items():
            print(k)
        return [k for k, v in self.__files.items() if k.rpartition('/')[0] == join(self.__dir, path)]
