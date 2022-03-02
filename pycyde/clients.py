import asyncio
import json
import logging
import multiprocessing
from contextlib import closing
from multiprocessing import Pool
from os.path import join, isfile, basename, isdir
from typing import List, Tuple, Optional

import httpx
import requests
from httpx import RequestError, ReadTimeout, TimeoutException
from requests import RequestException, ReadTimeout, Timeout, HTTPError
from requests.auth import HTTPBasicAuth
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

from pycyde.utils import pattern_matches, list_local_files


class TerrainClient:
    def __init__(self, access_token: str, timeout_seconds: int = 15):
        self.__logger = logging.getLogger(__name__)
        self.__token = access_token
        self.__timeout = timeout_seconds

    @property
    def access_token(self):
        return self.__token

    @property
    def timeout_seconds(self):
        return self.__timeout

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout)))
    def get_user_profile(self, username: str) -> dict:
        self.__logger.debug(f"Getting CyVerse profile for user {username}")
        response = requests.get(
            f"https://de.cyverse.org/terrain/secured/user-info?username={username}",
            headers={'Authorization': f"Bearer {self.__token}"},
            timeout=self.__timeout)

        response.raise_for_status()
        content = response.json()

        if username not in content: raise ValueError(f"Could not find user with username {username}")
        return content[username]

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout)))
    def get_path_info(self, path: str) -> dict:
        self.__logger.debug(f"Getting data store file {path}")
        response = requests.post(
            "https://de.cyverse.org/terrain/secured/filesystem/stat",
            data=json.dumps({'paths': [path]}),
            headers={'Authorization': f"Bearer {self.__token}",
                     "Content-Type": 'application/json;charset=utf-8'})

        if response.status_code == 500 and response.json()['error_code'] == 'ERR_DOES_NOT_EXIST':
            raise ValueError(f"Path {path} does not exist")
        elif response.status_code == 400:
            raise ValueError(f"Bad request: {response}")

        response.raise_for_status()
        content = response.json()
        return content['paths'][path]

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout)))
    def path_exists(self, path: str) -> bool:
        """
        Checks whether a collection (directory) or object (file) exists at the given path.

        Args:
            path: The path

        Returns: True if the path exists, otherwise False
        """

        self.__logger.debug(f"Checking if data store path exists: {path}")

        data = {'paths': [path]}
        headers = {"Authorization": f"Bearer {self.__token}", "Content-Type": "application/json;charset=utf-8"}
        response = requests.post("https://de.cyverse.org/terrain/secured/filesystem/exists",
                                 data=json.dumps(data),
                                 headers=headers,
                                 timeout=self.__timeout)

        # before invoking `raise_for_status` and bubbling an exception up,
        # try to decode the response and check the reason for failure
        if response.status_code != 200:
            try:
                content = response.json()
                self.__logger.warning(f"Bad response when checking if path '{path}' exists: {content}")
            finally:
                pass

        response.raise_for_status()
        content = response.json()
        if 'paths' not in content: raise ValueError(f"No paths on response: {content}")
        if path not in content['paths'].keys(): return False
        return content['paths'][path]

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout)))
    def path_exists_and_type(self, path: str) -> Tuple[bool, Optional[str]]:
        """
        Checks whether a collection (directory) or object (file) exists at the given path, and returns its type.

        Args:
            path: The path

        Returns: (True, type of object) if the path exists, otherwise (False, None)
        """

        self.__logger.debug(f"Checking if data store path exists (and type): {path}")

        data = {'paths': [path]}
        headers = {"Authorization": f"Bearer {self.__token}", "Content-Type": "application/json"}
        response = requests.post("https://de.cyverse.org/terrain/secured/filesystem/stat",
                                 data=json.dumps(data),
                                 headers=headers,
                                 timeout=self.__timeout)

        # before invoking `raise_for_status` and bubbling an exception up,
        # try to decode the response and check the reason for failure
        if response.status_code == 400:
            return False, None
        elif response.status_code != 200:
            try:
                content = response.json()
                self.__logger.warning(f"Bad response when checking if path '{path}' exists: {content}")
            finally:
                pass

        response.raise_for_status()
        content = response.json()
        if 'paths' not in content: raise ValueError(f"No paths on response: {content}")
        if path not in content['paths'].keys(): return False, None
        return True, content['paths'][path]['type']

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def dir_exists(self, path: str) -> bool:
        response = requests.post('https://de.cyverse.org/terrain/secured/filesystem/stat',
                                 headers={'Authorization': f"Bearer {self.__token}",
                                          "Content-Type": 'application/json;charset=utf-8'},
                                 data=json.dumps({'paths': [path]}),
                                 timeout=self.__timeout)

        if response.status_code == 500 and response.json()['error_code'] == 'ERR_DOES_NOT_EXIST':
            return False

        response.raise_for_status()
        content = response.json()

        if 'paths' not in content: raise ValueError(f"No paths in response: {content}")
        result = content['paths'][path]
        return result['type'] == 'dir'

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def file_exists(self, path: str) -> bool:
        response = requests.post('https://de.cyverse.org/terrain/secured/filesystem/stat',
                                 headers={'Authorization': f"Bearer {self.__token}",
                                          "Content-Type": 'application/json;charset=utf-8'},
                                 data=json.dumps({'paths': [path]}),
                                 timeout=self.__timeout)

        if response.status_code == 500 and response.json()['error_code'] == 'ERR_DOES_NOT_EXIST':
            return False

        response.raise_for_status()
        content = response.json()

        if 'paths' not in content: raise ValueError(f"No paths in response: {content}")
        result = content['paths'][path]
        return result['type'] == 'file'

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout)))
    def list_files(self, path: str) -> List[dict]:
        self.__logger.debug(f"Listing data store directory {path}")
        response = requests.get(
            f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={path}",
            headers={'Authorization': f"Bearer {self.__token}"},
            timeout=self.__timeout)

        if response.status_code == 500 and response.json()['error_code'] == 'ERR_DOES_NOT_EXIST':
            raise ValueError(f"Path {path} does not exist")

        response.raise_for_status()
        content = response.json()

        if 'files' not in content: raise ValueError(f"No files on response: {content}")
        return content['files']

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout)))
    def create_dir(self, path: str):
        self.__logger.debug(f"Creating data store directory {path}")
        headers = {
            "Authorization": f"Bearer {self.__token}",
        }
        response = requests.post("https://de.cyverse.org/terrain/secured/filesystem/directory/create",
                                     data=json.dumps({'path': path}),
                                 headers=headers,
                                 timeout=self.__timeout)
        response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout)))
    def share_dir(self, data: dict):
        self.__logger.debug(f"Sharing data store path(s): {json.dumps(data)}")
        headers = {
            "Authorization": f"Bearer {self.__token}",
            "Content-Type": "application/json;charset=utf-8"
        }
        response = requests.post("https://de.cyverse.org/terrain/secured/share",
                                 data=json.dumps(data),
                                 headers=headers,
                                 timeout=self.__timeout)
        response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout)))
    def unshare_dir(self, data: dict):
        self.__logger.debug(f"Unsharing data store path(s): {json.dumps(data)}")
        headers = {
            "Authorization": f"Bearer {self.__token}",
            "Content-Type": 'application/json;charset=utf-8'
        }
        response = requests.post("https://de.cyverse.org/terrain/secured/unshare",
                                 data=json.dumps(data),
                                 headers=headers,
                                 timeout=self.__timeout)
        response.raise_for_status()

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def verify_checksums(self, from_path: str, expected_pairs: List[dict]):
        response = requests.post('https://de.cyverse.org/terrain/secured/filesystem/stat',
                                 headers={'Authorization': f"Bearer {self.__token}"},
                                 data={'paths': expected_pairs},
                                 timeout=self.__timeout)

        response.raise_for_status()
        actual_pairs = [(path[join(from_path, path)]['label'], path[join(from_path, path)]['md5']) for path in
                        response.json()['paths']]

        if len(actual_pairs) != len(expected_pairs):
            raise ValueError(f"{len(expected_pairs)} file-checksum pairs provided but {len(actual_pairs)} files exist")

        for actual in actual_pairs:
            expected = [pair for pair in expected_pairs if pair['name'] == actual[0]][0]
            assert expected['file'] == actual[0]
            assert expected['checksum'] == actual[1]

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout) | retry_if_exception_type(HTTPError)))
    def pull_file(
            self,
            from_path: str,
            to_path: str,
            index: int = None,
            overwrite: bool = False):
        to_path_full = f"{to_path}/{from_path.split('/')[-1]}"
        if isfile(to_path_full) and not overwrite:
            self.__logger.info(f"File {to_path_full} already exists, skipping download")
            return
        else:
            if index is None:
                self.__logger.info(f"Downloading file '{from_path}' to '{to_path_full}'")
            else:
                self.__logger.info(f"Downloading file '{from_path}' to '{to_path_full}' ({index})")

        response = requests.get(f"https://de.cyverse.org/terrain/secured/fileio/download?path={from_path}",
                                headers={'Authorization': f"Bearer {self.__token}"},
                                timeout=self.__timeout)

        if response.status_code == 500 and response.json()['error_code'] == 'ERR_REQUEST_FAILED':
            raise ValueError(f"Path {from_path} does not exist")

        response.raise_for_status()
        with open(to_path_full, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestException) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(
            Timeout)))
    def push_file(self, from_path: str, to_prefix: str):
        self.__logger.debug(f"Uploading {from_path} to data store path {to_prefix}")
        with open(from_path, 'rb') as file:
            response = requests.post(f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={to_prefix}",
                                     headers={'Authorization': f"Bearer {self.__token}"},
                                     files={'file': (basename(from_path), file, 'application/octet-stream')},
                                     timeout=self.__timeout)

            if response.status_code == 500 and response.json()['error_code'] == 'ERR_EXISTS':
                self.__logger.warning(f"File '{join(to_prefix, basename(file.name))}' already exists, skipping upload")
            else:
                response.raise_for_status()

    def pull_dir(
            self,
            from_path: str,
            to_path: str,
            patterns: List[str] = None,
            checksums: List[dict] = None,
            overwrite: bool = False):
        """

        """

        check = checksums is not None and len(checksums) > 0
        paths = [file['path'] for file in self.list_files(from_path)]
        paths = [path for path in paths if pattern_matches(path, patterns)] if (
                patterns is not None and len(patterns) > 0) else paths
        num_paths = len(paths)

        # verify  that input checksums haven't changed since submission time
        if check: self.verify_checksums(from_path, checksums)

        print(f"Downloading directory '{from_path}' with {len(paths)} file(s)")
        with closing(Pool(processes=multiprocessing.cpu_count())) as pool:
            args = [(path, to_path, i) for i, path in enumerate(paths)]
            pool.starmap(self.pull_file, args)

        # TODO: verify that input checksums haven't changed since download time?
        # (maybe a bit excessive, and will add network latency, but probably prudent)
        # if check: verify_checksums(from_path, checksums)

    def push_dir(self,
                 from_path: str,
                 to_prefix: str,
                 include_patterns: List[str] = None,
                 include_names: List[str] = None,
                 exclude_patterns: List[str] = None,
                 exclude_names: List[str] = None):
        """
        Pushes the contents of the local directory to the given data store collection.
        Invokes `push_file()` internally, so no need for an outer retry policy.

        Args:
            from_path: The local directory path
            to_prefix: The data store collection path
            include_patterns: Filename patterns to include
            include_names: Filenames to include
            exclude_patterns: Filename patterns to exclude
            exclude_names: Filenames to exclude
        """

        # check path type
        is_file = isfile(from_path)
        is_dir = isdir(from_path)

        if not (is_dir or is_file):
            raise FileNotFoundError(f"Local path '{from_path}' does not exist")
        elif is_dir:
            from_paths = list_local_files(from_path, include_patterns, include_names, exclude_patterns, exclude_names)
            self.__logger.debug(f"Uploading directory '{from_path}' with {len(from_paths)} file(s) to '{to_prefix}'")
            with closing(Pool(processes=multiprocessing.cpu_count())) as pool:
                pool.starmap(self.push_file, [(path, to_prefix) for path in [str(p) for p in from_paths]])
        elif is_file:
            self.push_file(from_path, to_prefix)
        else:
            raise ValueError(f"Remote path '{to_prefix}' is a file; specify a directory path instead")


class AsyncTerrainClient:
    def __init__(self, access_token: str, timeout_seconds: int = 15):
        self.__logger = logging.getLogger(__name__)
        self.__token = access_token
        self.__timeout = timeout_seconds

    @property
    def access_token(self):
        return self.__token

    @property
    def timeout_seconds(self):
        return self.__timeout

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestError) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(TimeoutException)))
    async def create_dir_async(self, path: str):
        self.__logger.debug(f"Creating data store directory {path}")
        headers = {
            "Authorization": f"Bearer {self.__token}",
        }
        async with httpx.AsyncClient(headers=headers, timeout=self.__timeout) as client:
            response = await client.post("https://de.cyverse.org/terrain/secured/filesystem/directory/create",
                                         data=json.dumps({'path': path}))
            response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestError) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(TimeoutException)))
    async def get_dirs_async(self, paths: List[str]):
        self.__logger.debug(f"Listing data store directories: {', '.join(paths)}")
        urls = [f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={path}" for path in paths]
        headers = {
            "Authorization": f"Bearer {self.__token}",
        }
        async with httpx.AsyncClient(headers=headers, timeout=self.__timeout) as client:
            tasks = [client.get(url).json() for url in urls]
            results = await asyncio.gather(*tasks)
            return results

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestError) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(TimeoutException)))
    async def share_dir_async(self, data: dict):
        self.__logger.debug(f"Sharing data store path(s): {json.dumps(data)}")
        headers = {
            "Authorization": f"Bearer {self.__token}",
            "Content-Type": "application/json;charset=utf-8"
        }
        async with httpx.AsyncClient(headers=headers, timeout=self.__timeout) as client:
            response = await client.post("https://de.cyverse.org/terrain/secured/share", data=json.dumps(data))
            response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(retry_if_exception_type(ConnectionError) | retry_if_exception_type(
            RequestError) | retry_if_exception_type(ReadTimeout) | retry_if_exception_type(TimeoutException)))
    async def unshare_dir_async(self, data: dict):
        self.__logger.debug(f"Unsharing data store path(s): {json.dumps(data)}")
        headers = {
            "Authorization": f"Bearer {self.__token}",
            "Content-Type": 'application/json;charset=utf-8'
        }
        async with httpx.AsyncClient(headers=headers, timeout=self.__timeout) as client:
            response = await client.post("https://de.cyverse.org/terrain/secured/unshare",
                                         data=json.dumps(data))
            response.raise_for_status()