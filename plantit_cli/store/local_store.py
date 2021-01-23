from os.path import isfile, isdir, join
from pathlib import Path
from shutil import copyfileobj
from typing import List

from plantit_cli.store.store import Store
from plantit_cli.utils import list_files


class LocalStore(Store):
    def __init__(self, temp_dir):
        self.__files = {}
        self.__dir = temp_dir

    @property
    def dir(self):
        return self.__dir

    def dir_exists(self, path) -> bool:
        return Path(join(self.__dir, path)).is_dir()

    def file_exists(self, path) -> bool:
        return Path(join(self.__dir, path)).is_file()

    def list_dir(self, path) -> List[str]:
        return [k for k, v in self.__files.items() if k.rpartition('/')[0] == join(self.__dir, path)]

    def pull_file(self, from_path: str, to_path: str, overwrite: bool = False):
        from_path_file = join(self.__dir, from_path)
        to_path_file = join(to_path, from_path_file.split('/')[-1])

        with open(from_path_file, 'rb') as from_file, open(to_path_file, 'wb+') as to_file:
            print(f"Copying {from_path_file} to {to_path_file}")
            copyfileobj(from_file, to_file)

    def pull_dir(self, from_prefix: str, to_path: str, patterns: List[str], overwrite: bool = False):
        from_paths = [path for path in self.list_dir(from_prefix) if any(pattern.lower() in path.lower() for pattern in patterns)] \
            if patterns is not None else self.list_dir(from_prefix)

        for path in from_paths:
            self.pull_file(path, to_path)

    def push_file(self, from_path: str, to_prefix: str):
        to_path_dir = join(self.__dir, to_prefix)
        to_path_file = join(self.__dir, to_prefix, from_path.split('/')[-1])

        Path(to_path_dir).mkdir(parents=True, exist_ok=True)
        self.__files[to_path_file] = from_path

        with open(from_path, 'rb') as from_file, open(to_path_file, 'wb+') as to_file:
            print(f"Copying {from_path} to {to_path_file}")
            copyfileobj(from_file, to_file)

    def push_dir(self, from_path: str, to_prefix: str, include_patterns=None, include_names=None, exclude_patterns=None, exclude_names=None):
        is_file = isfile(from_path)
        is_dir = isdir(from_path)

        if not (is_dir or is_file):
            raise FileNotFoundError(f"Path '{from_path}' does not exist")
        elif is_dir:
            from_paths = list_files(from_path, include_patterns, include_names, exclude_patterns, exclude_names)
            print(f"Uploading files: {from_paths}")
            for path in [str(p) for p in from_paths]:
                self.push_file(path, to_prefix)
        elif is_file:
            self.push_file(from_path, to_prefix)
        else:
            raise ValueError(
                f"Path '{to_prefix}' is a file; specify a directory path instead")
