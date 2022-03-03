import logging
import traceback
from os import listdir, getcwd
from pathlib import Path
from typing import List

from pycyapi.clients import TerrainClient
from pycyapi.auth import AccessToken

logger = logging.getLogger(__name__)


def cas_token(username: str, password: str) -> str:
    if username is None or password is None or username == '' or password == '':
        raise ValueError(f"Username and password must be provided!")

    try:
        return AccessToken.get(username, password)
    except:
        logger.error(f"Token request failed: {traceback.format_exc()}")
        raise


def user_info(username: str, token: str = None):
    if token is not None:
        client = TerrainClient(token)
    else:
        raise ValueError(f"An authentication token must be explicitly provided for now")
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        return client.user_info(username)
    except:
        logger.error(f"User profile request failed: {traceback.format_exc()}")
        raise


def paged_directory(path: str, token: str = None):
    if token is not None:
        client = TerrainClient(token)
    else:
        raise ValueError(f"An authentication token must be explicitly provided for now")
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        return client.paged_directory(path)
    except:
        logger.error(f"Failed to list files: {traceback.format_exc()}")
        raise


def stat(path: str, token: str = None):
    pass


def exists(path: str, type: str = None, token: str = None):
    # TODO: use type with client.file_exists and client.dir_exists to check if type matches expectation
    pass


def mkdir(path: str, token: str = None):
    pass


def share(path: str, username: str, token: str = None):
    pass


def unshare(path: str, username: str, token: str = None):
    pass


def download(
        remote_path: str,
        local_path: str = None,
        patterns: List[str] = None,
        checksums: List[dict] = None,
        overwrite: bool = False,
        token: str = None):
    if token is not None:
        client = TerrainClient(token)
    else:
        raise ValueError(f"An authentication token must be explicitly provided for now")
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        local_path = getcwd() if (local_path is None or local_path == '') else local_path
        Path(local_path).mkdir(exist_ok=True)

        if client.dir_exists(remote_path):
            client.download_directory(
                from_path=remote_path,
                to_path=local_path,
                patterns=patterns,
                checksums=checksums,
                overwrite=overwrite)
        elif client.file_exists(remote_path):
            client.download(
                from_path=remote_path,
                to_path=local_path,
                overwrite=overwrite)
        else:
            msg = f"Path does not exist: {remote_path}"
            logger.error(msg)
            raise ValueError(msg)

        files = listdir(local_path)
        if len(files) == 0:
            msg = f"No files found at path '{remote_path}'" + f" matching patterns {patterns}"
            logger.error(msg)
            raise ValueError(msg)
        logger.info(f"Pulled input(s): {', '.join(files)}")
        return local_path
    except:
        logger.error(f"Pull failed: {traceback.format_exc()}")
        raise


def upload(
        local_path: str,
        remote_path: str,
        include_patterns: List[str] = None,
        include_names: List[str] = None,
        exclude_patterns: List[str] = None,
        exclude_names: List[str] = None,
        token: str = None):
    if token is not None:
        client = TerrainClient(token)
    else:
        raise ValueError(f"An authentication token must be explicitly provided for now")
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        client = TerrainClient(token)
        client.upload_directory(local_path, remote_path, include_patterns, include_names, exclude_patterns, exclude_names)
        logger.info(f"Pushed output(s)")
    except:
        logger.error(f"Failed to push outputs: {traceback.format_exc()}")
        raise
