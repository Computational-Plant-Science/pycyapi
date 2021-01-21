import multiprocessing
from contextlib import closing
from multiprocessing import Pool
from os.path import isdir, isfile, basename, join
from typing import List

import requests
from requests import ReadTimeout, Timeout, HTTPError, RequestException
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from plantit_cli.exceptions import PlantitException
from plantit_cli.options import PlantITCLIOptions
from plantit_cli.store.store import Store
from plantit_cli.utils import update_status, list_files


class TerrainStore(Store):
    def __init__(self, plan: PlantITCLIOptions):
        super().__init__(plan)

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def directory_exists(self, path) -> bool:
        with requests.post('https://de.cyverse.org/terrain/secured/filesystem/stat',
                           headers={'Authorization': f"Bearer {self.plan.cyverse_token}"},
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
    def file_exists(self, path) -> bool:
        with requests.post('https://de.cyverse.org/terrain/secured/filesystem/stat',
                           headers={'Authorization': f"Bearer {self.plan.cyverse_token}"},
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
    def list_directory(self, path) -> List[str]:
        with requests.get(
                f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={path}",
                headers={'Authorization': f"Bearer {self.plan.cyverse_token}"}) as response:
            if response.status_code == 500 and response.json()['error_code'] == 'ERR_DOES_NOT_EXIST':
                raise ValueError(f"Path {path} does not exist")
            response.raise_for_status()
            content = response.json()
            files = content['files']
            # print(f"Directory '{path}' contains {len(files)} files:")
            # for file in files:
            #     pprint(file['label'])
            return [file['path'] for file in files]

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def download_file(self, from_path, to_path):
        to_path_full = f"{to_path}/{from_path.split('/')[-1]}"
        if isfile(to_path_full) and ('overwrite' not in self.plan.input or (
                'overwrite' in self.plan.input and not self.plan.input['overwrite'])):
            print(f"File {to_path_full} already exists, skipping download")
            # update_status(self.plan, 3, f"File {to_path_full} already exists, skipping download")
            return
        else:
            print(f"Downloading '{from_path}' to '{to_path_full}'")
            # update_status(self.plan, 3, f"Downloading '{from_path}' to '{to_path_full}'")
        with requests.get(f"https://de.cyverse.org/terrain/secured/fileio/download?path={from_path}",
                          headers={'Authorization': f"Bearer {self.plan.cyverse_token}"}) as response:
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
    def __verify_inputs(self, from_path, paths):
        with requests.post('https://de.cyverse.org/terrain/secured/filesystem/stat',
                           headers={'Authorization': f"Bearer {self.plan.cyverse_token}"},
                           data={'paths': paths}) as response:
            response.raise_for_status()
            pairs = [(path[join(from_path, path)]['label'], path[join(from_path, path)]['md5']) for path in
                     response.json()['paths']]
            if len(pairs) != len(self.plan.checksums):
                raise PlantitException(
                    f"{len(self.plan.checksums)} checksum pairs provided but {len(pairs)} files exist")
            for actual in pairs:
                expected = [pair for pair in self.plan.checksums if pair['name'] == actual[0]][0]
                assert expected['name'] == actual[0]
                assert expected['md5'] == actual[1]

    def download_directory(self, from_path: str, to_path: str, patterns: List[str] = None):
        check = self.plan.checksums is not None and len(self.plan.checksums) > 0
        match = lambda path: any(pattern.lower() in path.lower() for pattern in patterns)
        paths = self.list_directory(from_path)
        paths = [path for path in paths if match(path)] if patterns is not None else paths

        # verify  that input checksums haven't changed since submission time
        if check:
            self.__verify_inputs(from_path, paths)

        update_status(self.plan, 3, f"Downloading directory '{from_path}' with {len(paths)} file(s)")
        with closing(Pool(processes=multiprocessing.cpu_count())) as pool:
            pool.starmap(self.download_file, [(path, to_path) for path in paths])

        # verify that input checksums haven't changed since download time
        # (maybe a bit excessive, and will add network latency, but probably prudent)
        if check:
            self.__verify_inputs(from_path, paths)

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def upload_file(self, from_path, to_path):
        print(f"Uploading '{from_path}' to '{to_path}'")
        with open(from_path, 'rb') as file:
            with requests.post(f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={to_path}",
                               headers={'Authorization': f"Bearer {self.plan.cyverse_token}"},
                               files={'file': (basename(from_path), file, 'application/octet-stream')}) as response:
                if response.status_code == 500 and response.json()['error_code'] == 'ERR_EXISTS':
                    update_status(self.plan, 3,
                                  f"File '{join(to_path, basename(file.name))}' already exists, skipping upload")
                else:
                    response.raise_for_status()

    def upload_directory(self,
                         from_path: str,
                         to_path: str,
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
            update_status(self.plan, 3,
                          f"Uploading directory '{from_path}' with {len(from_paths)} file(s) to '{to_path}'")
            with closing(Pool(processes=multiprocessing.cpu_count())) as pool:
                pool.starmap(self.upload_file, [(path, to_path) for path in [str(p) for p in from_paths]])
        elif is_file:
            self.upload_file(from_path, to_path)
        else:
            raise ValueError(
                f"Remote path '{to_path}' is a file; specify a directory path instead")
