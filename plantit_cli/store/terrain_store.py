import multiprocessing
from contextlib import closing
from multiprocessing import Pool
from os.path import isdir, isfile, basename, join
from typing import List

import requests
from requests import ReadTimeout, Timeout, HTTPError, RequestException
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from plantit_cli.exceptions import PlantitException
from plantit_cli.store.store import Store
from plantit_cli.utils import update_status, list_files


class FileChecksum():
    def __init__(self, file, checksum):
        self.file = file
        self.checksum = checksum


class TerrainStore(Store):
    def __init__(self, token: str):
        self.token = token

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def dir_exists(self, path: str) -> bool:
        with requests.post('https://de.cyverse.org/terrain/secured/filesystem/stat',
                           headers={'Authorization': f"Bearer {self.token}"},
                           data={'paths': [path]}) as response:
            if response.status_code == 500 and response.json()['error_code'] == 'ERR_DOES_NOT_EXIST':
                return False

            response.raise_for_status()
            content = response.json()
            result = [result[path] for result in content['paths']][0]
            return result['type'] == 'dir'

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def file_exists(self, path: str) -> bool:
        with requests.post('https://de.cyverse.org/terrain/secured/filesystem/stat',
                           headers={'Authorization': f"Bearer {self.token}"},
                           data={'paths': [path]}) as response:
            if response.status_code == 500 and response.json()['error_code'] == 'ERR_DOES_NOT_EXIST':
                return False

            response.raise_for_status()
            content = response.json()
            result = [result[path] for result in content['paths']][0]
            return result['type'] == 'file'


    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def list_dir(self, path: str) -> List[str]:
        with requests.get(
                f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={path}",
                headers={'Authorization': f"Bearer {self.token}"}) as response:
            if response.status_code == 500 and response.json()['error_code'] == 'ERR_DOES_NOT_EXIST':
                raise ValueError(f"Path {path} does not exist")

            response.raise_for_status()
            content = response.json()
            files = content['files']
            return [file['path'] for file in files]

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def pull_file(self, from_path: str, to_path: str, overwrite: bool = False):
        to_path_full = f"{to_path}/{from_path.split('/')[-1]}"

        if isfile(to_path_full) and not overwrite:
            print(f"File {to_path_full} already exists, skipping download")
            return
        else:
            print(f"Downloading '{from_path}' to '{to_path_full}'")

        with requests.get(f"https://de.cyverse.org/terrain/secured/fileio/download?path={from_path}",
                          headers={'Authorization': f"Bearer {self.token}"}) as response:
            if response.status_code == 500 and response.json()['error_code'] == 'ERR_REQUEST_FAILED':
                raise PlantitException(f"Path {from_path} does not exist")
            response.raise_for_status()
            with open(to_path_full, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def __verify_checksums(self, from_path: str, expected_pairs: List[FileChecksum]):
        with requests.post('https://de.cyverse.org/terrain/secured/filesystem/stat',
                           headers={'Authorization': f"Bearer {self.token}"},
                           data={'paths': expected_pairs}) as response:
            response.raise_for_status()
            actual_pairs = [(path[join(from_path, path)]['label'], path[join(from_path, path)]['md5']) for path in response.json()['paths']]

            if len(actual_pairs) != len(expected_pairs):
                raise ValueError(f"{len(expected_pairs)} file-checksum pairs provided but {len(actual_pairs)} files exist")

            for actual in actual_pairs:
                expected = [pair for pair in expected_pairs if pair['name'] == actual[0]][0]
                assert expected.file == actual[0]
                assert expected.checksum == actual[1]

    def pull_dir(self, from_path: str, to_path: str, patterns: List[str] = None, checksums: List[FileChecksum] = None, overwrite: bool = False):
        check = checksums is not None and len(checksums) > 0
        match = lambda path: any(pattern.lower() in path.lower() for pattern in patterns)
        paths = self.list_dir(from_path)
        paths = [path for path in paths if match(path)] if patterns is not None else paths

        # verify  that input checksums haven't changed since submission time
        if check:
            self.__verify_checksums(from_path, checksums)

        update_status(self.plan, 3, f"Downloading directory '{from_path}' with {len(paths)} file(s)")
        with closing(Pool(processes=multiprocessing.cpu_count())) as pool:
            pool.starmap(self.pull_file, [(path, to_path) for path in paths])

        # verify that input checksums haven't changed since download time
        # (maybe a bit excessive, and will add network latency, but probably prudent)
        if check:
            self.__verify_checksums(from_path, paths)

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def push_file(self, from_path: str, to_prefix: str):
        print(f"Uploading '{from_path}' to '{to_prefix}'")
        with open(from_path, 'rb') as file:
            with requests.post(f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={to_prefix}",
                               headers={'Authorization': f"Bearer {self.token}"},
                               files={'file': (basename(from_path), file, 'application/octet-stream')}) as response:
                if response.status_code == 500 and response.json()['error_code'] == 'ERR_EXISTS':
                    print(f"File '{join(to_prefix, basename(file.name))}' already exists, skipping upload")
                else:
                    response.raise_for_status()

    def push_dir(self,
                 from_path: str,
                 to_prefix: str,
                 include_patterns: List[str] = None,
                 include_names: List[str] = None,
                 exclude_patterns: List[str] = None,
                 exclude_names: List[str] = None):
        is_file = isfile(from_path)
        is_dir = isdir(from_path)

        if not (is_dir or is_file):
            raise FileNotFoundError(f"Local path '{from_path}' does not exist")
        elif is_dir:
            from_paths = list_files(from_path, include_patterns, include_names, exclude_patterns, exclude_names)
            print(f"Uploading directory '{from_path}' with {len(from_paths)} file(s) to '{to_prefix}'")
            with closing(Pool(processes=multiprocessing.cpu_count())) as pool:
                pool.starmap(self.push_file, [(path, to_prefix) for path in [str(p) for p in from_paths]])
        elif is_file:
            self.push_file(from_path, to_prefix)
        else:
            raise ValueError(f"Remote path '{to_prefix}' is a file; specify a directory path instead")
