import asyncio
import json
import logging
from os.path import basename, isdir, isfile, join
from typing import Dict, List

import httpx
import requests
from httpx import RequestError, TimeoutException
from requests import HTTPError, ReadTimeout, RequestException, Timeout
from requests.auth import HTTPBasicAuth
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from pycyapi.cyverse.exceptions import (
    BadRequest,
    BadResponse,
    NotFound,
    Unauthorized,
)
from pycyapi.utils import list_local_files, pattern_matches


class CyverseClient:
    def __init__(self, access_token: str, timeout_seconds: int = 15):
        self.__logger = logging.getLogger(__name__)
        self.__access_token = access_token
        self.__timeout = timeout_seconds

    @property
    def access_token(self):
        return self.__access_token

    @property
    def timeout_seconds(self):
        return self.__timeout

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def user_info(self, username: str) -> dict:
        self.__logger.debug(f"Getting CyVerse profile for user {username}")
        response = requests.get(
            f"https://de.cyverse.org/terrain/secured/user-info?username={username}",
            headers={"Authorization": f"Bearer {self.__access_token}"},
            timeout=self.__timeout,
        )

        response.raise_for_status()
        content = response.json()
        info = content.get(username, None)
        if info is None:
            raise ValueError(
                f"Could not find user with username {username} in response: {content}"
            )
        return info

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def refresh_tokens(
        self,
        username: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        redirect_uri: str,
    ) -> (str, str):
        self.__logger.debug(f"Refreshing CyVerse tokens for user {username}")
        response = requests.post(
            "https://kc.cyverse.org/auth/realms/CyVerse/protocol/openid-connect/token",
            data={
                "grant_type": "refresh_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token,
                "redirect_uri": redirect_uri,
            },
            auth=HTTPBasicAuth(username, client_secret),
        )

        if response.status_code == 400:
            raise Unauthorized("Unauthorized for KeyCloak token endpoint")
        elif response.status_code != 200:
            raise BadResponse(
                f"Bad response from KeyCloak token endpoint:\n{response.json()}"
            )

        content = response.json()
        if "access_token" not in content or "refresh_token" not in content:
            raise BadRequest(
                f"Missing params on token response, expected 'access_token' and 'refresh_token' but got:\n{content}"
            )

        return content["access_token"], content["refresh_token"]

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def stat(self, path: str) -> dict:
        self.__logger.debug(f"Getting data store file {path}")
        response = requests.post(
            "https://de.cyverse.org/terrain/secured/filesystem/stat",
            data=json.dumps({"paths": [path]}),
            headers={
                "Authorization": f"Bearer {self.__access_token}",
                "Content-Type": "application/json;charset=utf-8",
            },
        )

        if (
            response.status_code == 500
            and response.json()["error_code"] == "ERR_DOES_NOT_EXIST"
        ):
            raise NotFound(f"Path {path} does not exist")
        elif response.status_code == 400:
            raise ValueError(f"Bad request: {response}")

        response.raise_for_status()
        content = response.json()
        paths = content.get("paths", None)
        if paths is None:
            raise ValueError(f"No paths on response: {content}")
        info = paths.get(path, None)
        if info is None:
            raise NotFound(f"No path info on response: {content}")
        return info

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def exists(self, path: str) -> bool:
        """
        Checks whether a collection (directory) or object (file) exists at the given path.

        Args:
            path: The path

        Returns: True if the path exists, otherwise False
        """

        self.__logger.debug(f"Checking if data store path exists: {path}")

        data = {"paths": [path]}
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }
        response = requests.post(
            "https://de.cyverse.org/terrain/secured/filesystem/exists",
            data=json.dumps(data),
            headers=headers,
            timeout=self.__timeout,
        )

        # before invoking `raise_for_status` and bubbling an exception up,
        # try to decode the response and check the reason for failure
        if response.status_code != 200:
            try:
                content = response.json()
                self.__logger.warning(
                    f"Bad response when checking if path '{path}' exists: {content}"
                )
            finally:
                pass

        response.raise_for_status()
        content = response.json()
        if "paths" not in content:
            raise ValueError(f"No paths on response: {content}")
        if path not in content["paths"].keys():
            return False
        return content["paths"][path]

    def dir_exists(self, path: str) -> bool:
        try:
            return self.stat(path)["type"] == "dir"
        except NotFound:
            return False

    def file_exists(self, path: str) -> bool:
        try:
            return self.stat(path)["type"] == "file"
        except NotFound:
            return False

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def list(self, path: str) -> dict:
        self.__logger.debug(f"Listing data store directory {path}")
        response = requests.get(
            f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={path}",
            headers={"Authorization": f"Bearer {self.__access_token}"},
            timeout=self.__timeout,
        )

        if (
            response.status_code == 500
            and response.json()["error_code"] == "ERR_DOES_NOT_EXIST"
        ):
            raise ValueError(f"Path {path} does not exist")

        response.raise_for_status()
        directory = response.json()
        return directory

    def list_files(self, path: str):
        directory = self.list(path)
        if "files" not in directory:
            raise ValueError(f"No files on response: {directory}")
        return directory["files"]

    def list_folders(self, path: str):
        directory = self.list(path)
        if "folders" not in directory:
            raise ValueError(f"No folders on response: {directory}")
        return directory["folders"]

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def mkdir(self, path: str):
        self.__logger.debug(f"Creating data store directory {path}")
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
        }
        response = requests.post(
            "https://de.cyverse.org/terrain/secured/filesystem/directory/create",
            data=json.dumps({"path": path}),
            headers=headers,
            timeout=self.__timeout,
        )
        response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def share(self, username: str, path: str, permission: str):
        # validate permission
        if permission != "read" and permission != "write":
            raise ValueError(f"Invalid permission (must be 'read' or 'write')")

        # compose request body and headers
        data = {
            "sharing": [
                {
                    "user": username,
                    "paths": [{"path": path, "permission": permission}],
                }
            ]
        }
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(f"Sharing data store path(s): {json.dumps(data)}")
        response = requests.post(
            "https://de.cyverse.org/terrain/secured/share",
            data=json.dumps(data),
            headers=headers,
            timeout=self.__timeout,
        )
        response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def unshare(self, username: str, path: str):
        # compose request body and headers
        data = {"unshare": [{"user": username, "paths": [path]}]}
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(
            f"Unsharing data store path(s): {json.dumps(data)}"
        )
        response = requests.post(
            "https://de.cyverse.org/terrain/secured/unshare",
            data=json.dumps(data),
            headers=headers,
            timeout=self.__timeout,
        )
        response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def share_many(self, username: str, paths: List[Dict[str, str]]):
        # validate path/permission pairs
        for p in paths:
            path = p.get("path", None)
            permission = p.get("permission", None)
            if not path.startswith(f"/iplant/home/"):
                raise ValueError(f"A path is empty or invalid")
            if permission != "read" and permission != "write":
                raise ValueError(
                    f"Invalid permissions (must be 'read' or 'write')"
                )

        # compose request body and headers
        data = {"sharing": [{"user": username, "paths": paths}]}
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(f"Sharing data store path(s): {json.dumps(data)}")
        response = requests.post(
            "https://de.cyverse.org/terrain/secured/share",
            data=json.dumps(data),
            headers=headers,
            timeout=self.__timeout,
        )
        response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def unshare_many(self, username: str, paths: List[str]):
        # validate path/permission pairs
        for p in paths:
            if not p.startswith(f"/iplant/home/"):
                raise ValueError(f"A path is empty or invalid")

        # compose request body
        data = {"unshare": [{"user": username, "paths": paths}]}
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(
            f"Unsharing data store path(s): {json.dumps(data)}"
        )
        response = requests.post(
            "https://de.cyverse.org/terrain/secured/unshare",
            data=json.dumps(data),
            headers=headers,
            timeout=self.__timeout,
        )
        response.raise_for_status()

    def verify_checksums(self, from_path: str, expected_pairs: List[dict]):
        info = self.stat(from_path)
        pairs = [
            (
                path[join(from_path, path)]["label"],
                path[join(from_path, path)]["md5"],
            )
            for path in info["paths"]
        ]

        if len(pairs) != len(expected_pairs):
            raise ValueError(
                f"{len(expected_pairs)} file-checksum pairs provided but {len(pairs)} files exist"
            )

        for actual in pairs:
            expected = [
                pair for pair in expected_pairs if pair["name"] == actual[0]
            ][0]
            assert expected["file"] == actual[0]
            assert expected["checksum"] == actual[1]

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
            | retry_if_exception_type(HTTPError)
        ),
    )
    def download(
        self,
        from_path: str,
        to_path: str,
        index: int = None,
        overwrite: bool = False,
    ):
        # abort the download if the file already exists and the overwrite flag is false
        to_path_full = f"{to_path}/{from_path.split('/')[-1]}"
        if isfile(to_path_full) and not overwrite:
            self.__logger.info(
                f"File {to_path_full} already exists, skipping download"
            )
            return

        # send the request
        self.__logger.info(
            f"Downloading file '{from_path}' to '{to_path_full}'"
            + ("" if index is None else f" ({index})")
        )
        response = requests.get(
            f"https://de.cyverse.org/terrain/secured/fileio/download?path={from_path}",
            headers={"Authorization": f"Bearer {self.__access_token}"},
            timeout=self.__timeout,
        )

        if (
            response.status_code == 500
            and response.json()["error_code"] == "ERR_REQUEST_FAILED"
        ):
            raise ValueError(f"Path {from_path} does not exist")

        response.raise_for_status()
        with open(to_path_full, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def upload(self, from_path: str, to_prefix: str):
        self.__logger.debug(
            f"Uploading {from_path} to data store path {to_prefix}"
        )
        with open(from_path, "rb") as file:
            # compose request files and headers
            files = {
                "file": (basename(from_path), file, "application/octet-stream")
            }
            headers = {"Authorization": f"Bearer {self.__access_token}"}

            # send the request
            response = requests.post(
                f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={to_prefix}",
                headers=headers,
                files=files,
                timeout=self.__timeout,
            )

            if (
                response.status_code == 500
                and response.json()["error_code"] == "ERR_EXISTS"
            ):
                self.__logger.warning(
                    f"File '{join(to_prefix, basename(file.name))}' already exists, skipping upload"
                )
            else:
                response.raise_for_status()

    def download_directory(
        self,
        from_path: str,
        to_path: str,
        patterns: List[str] = None,
        checksums: List[dict] = None,
        overwrite: bool = False,
    ):
        check = checksums is not None and len(checksums) > 0
        paths = [file["path"] for file in self.list(from_path)["files"]]
        paths = (
            [path for path in paths if pattern_matches(path, patterns)]
            if (patterns is not None and len(patterns) > 0)
            else paths
        )
        num_paths = len(paths)

        # verify  that input checksums haven't changed since submission time
        if check:
            self.verify_checksums(from_path, checksums)

        self.__logger.info(
            f"Downloading directory '{from_path}' with {len(paths)} file(s)"
        )
        for i, from_path in enumerate(paths):
            self.download(from_path, to_path, i)

        # TODO: verify that input checksums haven't changed since download time?
        # (maybe a bit excessive, and will add network latency, but probably prudent)
        # if check: verify_checksums(from_path, checksums)

    def upload_directory(
        self,
        from_path: str,
        to_prefix: str,
        include_patterns: List[str] = None,
        include_names: List[str] = None,
        exclude_patterns: List[str] = None,
        exclude_names: List[str] = None,
    ):
        # check path type
        is_file = isfile(from_path)
        is_dir = isdir(from_path)

        if not (is_dir or is_file):
            raise FileNotFoundError(f"Local path '{from_path}' does not exist")
        elif is_dir:
            paths = list_local_files(
                from_path,
                include_patterns,
                include_names,
                exclude_patterns,
                exclude_names,
            )
            self.__logger.debug(
                f"Uploading directory '{from_path}' with {len(paths)} file(s) to '{to_prefix}'"
            )
            for from_path in paths:
                self.upload(from_path, to_prefix)
        elif is_file:
            self.upload(from_path, to_prefix)
        else:
            raise ValueError(
                f"Remote path '{to_prefix}' is a file; specify a directory path instead"
            )

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def get_metadata(self, id: str, irods: bool = False):
        # compose request headers
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(f"Getting metadata for data object with ID {id}")
        response = requests.get(
            f"https://de.cyverse.org/terrain/secured/filesystem/{id}/metadata",
            headers=headers,
            timeout=self.__timeout,
        )
        response.raise_for_status()
        content = response.json()
        return content["irods-avus"] if irods else content["avus"]

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    def set_metadata(
        self, id: str, attributes: List[str], irods_attributes: List[str]
    ):
        def to_avu(attr: str):
            split = attr.strip().split("=")
            return {"attr": split[0], "value": split[1], "unit": ""}

        # compose request body and headers
        data = {
            "avus": [to_avu(a) for a in attributes],
            "irods-avus": [to_avu(a) for a in irods_attributes],
        }
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(
            f"Setting metadata for data object with ID {id}: {json.dumps(data)}"
        )
        response = requests.post(
            f"https://de.cyverse.org/terrain/secured/filesystem/{id}/metadata",
            data=json.dumps(data),
            headers=headers,
            timeout=self.__timeout,
        )
        response.raise_for_status()


class AsyncCyverseClient:
    def __init__(self, access_token: str, timeout_seconds: int = 15):
        self.__logger = logging.getLogger(__name__)
        self.__access_token = access_token
        self.__timeout = timeout_seconds

    @property
    def access_token(self):
        return self.__access_token

    @property
    def timeout_seconds(self):
        return self.__timeout

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    async def user_info_async(self, username: str) -> dict:
        self.__logger.debug(f"Getting CyVerse profile for user {username}")
        async with httpx.AsyncClient(
            headers={"Authorization": f"Bearer {self.__access_token}"},
            timeout=self.__timeout,
        ) as client:
            response = await client.get(
                f"https://de.cyverse.org/terrain/secured/user-info?username={username}"
            )
            response.raise_for_status()
            content = response.json()
            info = content.get(username, None)
            if info is None:
                raise ValueError(
                    f"Could not find user with username {username} in response: {content}"
                )
            return info

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    async def refresh_tokens_async(
        self,
        username: str,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        redirect_uri: str,
    ) -> (str, str):
        self.__logger.debug(f"Refreshing CyVerse tokens for user {username}")
        async with httpx.AsyncClient(timeout=self.__timeout) as client:
            response = await client.post(
                "https://kc.cyverse.org/auth/realms/CyVerse/protocol/openid-connect/token",
                data={
                    "grant_type": "refresh_token",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": refresh_token,
                    "redirect_uri": redirect_uri,
                },
                auth=(username, client_secret),
            )

            if response.status_code == 400:
                raise Unauthorized("Unauthorized for KeyCloak token endpoint")
            elif response.status_code != 200:
                raise BadResponse(
                    f"Bad response from KeyCloak token endpoint:\n{response.json()}"
                )

            content = response.json()
            if "access_token" not in content or "refresh_token" not in content:
                raise BadRequest(
                    f"Missing params on token response, expected 'access_token' and 'refresh_token' but got:\n{content}"
                )

            return content["access_token"], content["refresh_token"]

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    async def stat_async(self, path: str) -> dict:
        # compose request body and headers
        data = {"paths": [path]}
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(f"Getting data store file {path}")
        async with httpx.AsyncClient(
            headers=headers, timeout=self.__timeout
        ) as client:
            response = await client.post(
                "https://de.cyverse.org/terrain/secured/filesystem/stat",
                data=json.dumps(data),
            )

            if (
                response.status_code == 500
                and response.json()["error_code"] == "ERR_DOES_NOT_EXIST"
            ):
                raise NotFound(f"Path {path} does not exist")
            elif response.status_code == 400:
                raise ValueError(f"Bad request: {response}")

            response.raise_for_status()
            content = response.json()
            paths = content.get("paths", None)
            if paths is None:
                raise ValueError(f"No paths on response: {content}")
            info = paths.get(path, None)
            if info is None:
                raise NotFound(f"No path info on response: {content}")
            return info

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    async def exists_async(self, path: str) -> bool:
        """
        Checks whether a collection (directory) or object (file) exists at the given path.

        Args:
            path: The path

        Returns: True if the path exists, otherwise False
        """

        # compose request body and headers
        data = {"paths": [path]}
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(f"Checking if data store path exists: {path}")
        async with httpx.AsyncClient(
            headers=headers, timeout=self.__timeout
        ) as client:
            response = await client.post(
                "https://de.cyverse.org/terrain/secured/filesystem/exists",
                data=json.dumps(data),
            )

            # before invoking `raise_for_status` and bubbling an exception up,
            # try to decode the response and check the reason for failure
            if response.status_code != 200:
                try:
                    content = response.json()
                    self.__logger.warning(
                        f"Bad response when checking if path '{path}' exists: {content}"
                    )
                finally:
                    pass

            response.raise_for_status()
            content = response.json()
            if "paths" not in content:
                raise ValueError(f"No paths on response: {content}")
            if path not in content["paths"].keys():
                return False
            return content["paths"][path]

    async def dir_exists_async(self, path: str) -> bool:
        try:
            info = await self.stat_async(path)
            return info["type"] == "dir"
        except NotFound:
            return False

    async def file_exists_async(self, path: str) -> bool:
        try:
            info = await self.stat_async(path)
            return info["type"] == "file"
        except NotFound:
            return False

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    async def list_async(self, path: str) -> dict:
        self.__logger.debug(f"Listing data store directory {path}")
        async with httpx.AsyncClient(
            headers={"Authorization": f"Bearer {self.__access_token}"},
            timeout=self.__timeout,
        ) as client:
            response = await client.get(
                f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={path}"
            )
            response.raise_for_status()

            if (
                response.status_code == 500
                and response.json()["error_code"] == "ERR_DOES_NOT_EXIST"
            ):
                raise ValueError(f"Path {path} does not exist")

            response.raise_for_status()
            directory = response.json()
            return directory

    async def list_files_async(self, path: str):
        directory = await self.list_async(path)
        if "files" not in directory:
            raise ValueError(f"No files on response: {directory}")
        return directory["files"]

    async def list_folders_async(self, path: str):
        directory = await self.list_async(path)
        if "folders" not in directory:
            raise ValueError(f"No folders on response: {directory}")
        return directory["folders"]

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestError)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(TimeoutException)
        ),
    )
    async def mkdir_async(self, path: str):
        self.__logger.debug(f"Creating data store directory {path}")
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
        }
        async with httpx.AsyncClient(
            headers=headers, timeout=self.__timeout
        ) as client:
            response = await client.post(
                "https://de.cyverse.org/terrain/secured/filesystem/directory/create",
                data=json.dumps({"path": path}),
            )
            response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    async def share_async(self, username: str, path: str, permission: str):
        # validate permission and path
        if not path.startswith(f"/iplant/home/"):
            raise ValueError(f"A path is empty or invalid")
        if permission != "read" and permission != "write":
            raise ValueError(f"Invalid permission (must be 'read' or 'write')")

        # compose request body and headers
        data = {
            "sharing": [
                {
                    "user": username,
                    "paths": [{"path": path, "permission": permission}],
                }
            ]
        }
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(f"Sharing data store path(s): {json.dumps(data)}")
        async with httpx.AsyncClient(
            headers=headers, timeout=self.__timeout
        ) as client:
            response = await client.post(
                "https://de.cyverse.org/terrain/secured/share",
                data=json.dumps(data),
            )
            response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    async def unshare_async(self, username: str, path: str):
        # validate path
        if not path.startswith(f"/iplant/home/"):
            raise ValueError(f"A path is empty or invalid")

        # compose request body and headers
        data = {"unshare": [{"user": username, "paths": [path]}]}
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(
            f"Unsharing data store path(s): {json.dumps(data)}"
        )
        async with httpx.AsyncClient(
            headers=headers, timeout=self.__timeout
        ) as client:
            response = await client.post(
                "https://de.cyverse.org/terrain/secured/unshare",
                data=json.dumps(data),
            )
            response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestError)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(TimeoutException)
        ),
    )
    async def share_many_async(
        self, username: str, paths: List[Dict[str, str]]
    ):
        # validate path/permission pairs
        for p in paths:
            path = p.get("path", None)
            permission = p.get("permission", None)
            if not path.startswith(f"/iplant/home/"):
                raise ValueError(f"A path is empty or invalid")
            if permission != "read" and permission != "write":
                raise ValueError(
                    f"Invalid permissions (must be 'read' or 'write')"
                )

        # compose request body and headers
        data = {"sharing": [{"user": username, "paths": paths}]}
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(f"Sharing data store path(s): {json.dumps(data)}")
        async with httpx.AsyncClient(
            headers=headers, timeout=self.__timeout
        ) as client:
            response = await client.post(
                "https://de.cyverse.org/terrain/secured/share",
                data=json.dumps(data),
            )
            response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestError)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(TimeoutException)
        ),
    )
    async def unshare_many_async(self, username: str, paths: List[str]):
        # validate path/permission pairs
        for p in paths:
            if not p.startswith(f"/iplant/home/"):
                raise ValueError(f"A path is empty or invalid")

        # compose request body and headers
        data = {"unshare": [{"user": username, "paths": paths}]}
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(
            f"Unsharing data store path(s): {json.dumps(data)}"
        )
        async with httpx.AsyncClient(
            headers=headers, timeout=self.__timeout
        ) as client:
            response = await client.post(
                "https://de.cyverse.org/terrain/secured/unshare",
                data=json.dumps(data),
            )
            response.raise_for_status()

    def verify_checksums(self, from_path: str, expected_pairs: List[dict]):
        info = self.stat_async(from_path)
        pairs = [
            (
                path[join(from_path, path)]["label"],
                path[join(from_path, path)]["md5"],
            )
            for path in info["paths"]
        ]

        if len(pairs) != len(expected_pairs):
            raise ValueError(
                f"{len(expected_pairs)} file-checksum pairs provided but {len(pairs)} files exist"
            )

        for actual in pairs:
            expected = [
                pair for pair in expected_pairs if pair["name"] == actual[0]
            ][0]
            assert expected["file"] == actual[0]
            assert expected["checksum"] == actual[1]

    @retry(
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
            | retry_if_exception_type(HTTPError)
        ),
    )
    async def download_async(
        self,
        from_path: str,
        to_path: str,
        index: int = None,
        overwrite: bool = False,
    ):
        # abort the download if the file already exists and the overwrite flag is false
        to_path_full = f"{to_path}/{from_path.split('/')[-1]}"
        if isfile(to_path_full) and not overwrite:
            self.__logger.info(
                f"File {to_path_full} already exists, skipping download"
            )
            return

        # send the request
        self.__logger.info(
            f"Downloading file '{from_path}' to '{to_path_full}'"
            + ("" if index is None else f" ({index})")
        )
        async with httpx.AsyncClient(
            headers={"Authorization": f"Bearer {self.__access_token}"},
            timeout=self.__timeout,
        ) as client:
            async with client.stream(
                "GET",
                f"https://de.cyverse.org/terrain/secured/fileio/download?path={from_path}",
            ) as response:
                if (
                    response.status_code == 500
                    and response.json()["error_code"] == "ERR_REQUEST_FAILED"
                ):
                    raise ValueError(f"Path {from_path} does not exist")

                response.raise_for_status()
                with open(to_path_full, "wb") as file:
                    async for chunk in response.aiter_bytes():
                        file.write(chunk)

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    async def upload_async(self, from_path: str, to_prefix: str):
        with open(from_path, "rb") as file:
            # compose request files and headers
            files = {
                "file": (basename(from_path), file, "application/octet-stream")
            }
            headers = {"Authorization": f"Bearer {self.__access_token}"}

            # send the request
            self.__logger.debug(
                f"Uploading {from_path} to data store path {to_prefix}"
            )
            async with httpx.AsyncClient(
                headers=headers, timeout=self.__timeout
            ) as client:
                response = await client.post(
                    f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={to_prefix}",
                    files=files,
                )

                if (
                    response.status_code == 500
                    and response.json()["error_code"] == "ERR_EXISTS"
                ):
                    self.__logger.warning(
                        f"File '{join(to_prefix, basename(file.name))}' already exists, skipping upload"
                    )
                else:
                    response.raise_for_status()

    async def download_directory_async(
        self,
        from_path: str,
        to_path: str,
        patterns: List[str] = None,
        checksums: List[dict] = None,
        overwrite: bool = False,
    ):
        check = checksums is not None and len(checksums) > 0
        files = (await self.list_async(from_path))["files"]
        paths = [file["path"] for file in files]
        paths = (
            [path for path in paths if pattern_matches(path, patterns)]
            if (patterns is not None and len(patterns) > 0)
            else paths
        )
        num_paths = len(paths)

        # verify  that input checksums haven't changed since submission time
        if check:
            self.verify_checksums(from_path, checksums)

        self.__logger.info(
            f"Downloading directory '{from_path}' with {len(paths)} file(s)"
        )
        for i, from_path in enumerate(paths):
            await self.download_async(from_path, to_path, i)

        # TODO: verify that input checksums haven't changed since download time?
        # (maybe a bit excessive, and will add network latency, but probably prudent)
        # if check: verify_checksums(from_path, checksums)

    async def upload_directory_async(
        self,
        from_path: str,
        to_prefix: str,
        include_patterns: List[str] = None,
        include_names: List[str] = None,
        exclude_patterns: List[str] = None,
        exclude_names: List[str] = None,
    ):
        # check path type
        is_file = isfile(from_path)
        is_dir = isdir(from_path)

        if not (is_dir or is_file):
            raise FileNotFoundError(f"Local path '{from_path}' does not exist")
        elif is_dir:
            paths = list_local_files(
                from_path,
                include_patterns,
                include_names,
                exclude_patterns,
                exclude_names,
            )
            self.__logger.debug(
                f"Uploading directory '{from_path}' with {len(paths)} file(s) to '{to_prefix}'"
            )
            for from_path in paths:
                await self.upload_async(from_path, to_prefix)
        elif is_file:
            await self.upload_async(from_path, to_prefix)
        else:
            raise ValueError(
                f"Remote path '{to_prefix}' is a file; specify a directory path instead"
            )

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    async def get_metadata_async(self, id: str, irods: bool = False):
        # compose request headers
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(f"Getting metadata for data object with ID {id}")
        async with httpx.AsyncClient(
            headers=headers, timeout=self.__timeout
        ) as client:
            response = await client.get(
                f"https://de.cyverse.org/terrain/secured/filesystem/{id}/metadata"
            )
            response.raise_for_status()
            content = response.json()
            return content["irods-avus"] if irods else content["avus"]

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    async def set_metadata_async(
        self, id: str, attributes: List[str], irods_attributes: List[str]
    ):
        def to_avu(attr: str):
            split = attr.strip().split("=")
            return {"attr": split[0], "value": split[1], "unit": ""}

        # compose request body and headers
        data = {
            "avus": [to_avu(a) for a in attributes],
            "irods-avus": [to_avu(a) for a in irods_attributes],
        }
        headers = {
            "Authorization": f"Bearer {self.__access_token}",
            "Content-Type": "application/json;charset=utf-8",
        }

        # send the request
        self.__logger.debug(
            f"Setting metadata for data object with ID {id}: {json.dumps(data)}"
        )
        async with httpx.AsyncClient(
            headers=headers, timeout=self.__timeout
        ) as client:
            response = await client.post(
                f"https://de.cyverse.org/terrain/secured/filesystem/{id}/metadata",
                data=json.dumps(data),
            )
            response.raise_for_status()

    @retry(
        reraise=True,
        wait=wait_exponential(multiplier=1, min=4, max=10),
        stop=stop_after_attempt(3),
        retry=(
            retry_if_exception_type(ConnectionError)
            | retry_if_exception_type(RequestException)
            | retry_if_exception_type(ReadTimeout)
            | retry_if_exception_type(Timeout)
        ),
    )
    async def get_dirs_async(
        self, paths: List[str], token: str, timeout: int = 15
    ):
        self.__logger.debug(
            f"Listing data store directories: {', '.join(paths)}"
        )
        urls = [
            f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?limit=1000&path={path}"
            for path in paths
        ]
        headers = {"Authorization": f"Bearer {token}"}
        async with httpx.AsyncClient(
            headers=headers, timeout=timeout
        ) as client:
            tasks = [client.get(url).json() for url in urls]
            results = await asyncio.gather(*tasks)
            return results
