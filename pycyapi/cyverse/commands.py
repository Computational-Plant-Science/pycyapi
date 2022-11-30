import logging
import traceback
from os import getcwd, listdir
from pathlib import Path
from typing import List

from pycyapi.cyverse.auth import CyverseAccessToken
from pycyapi.cyverse.clients import CyverseClient

logger = logging.getLogger(__file__)


def cas_token(username: str, password: str) -> str:
    if (
        username is None
        or password is None
        or username == ""
        or password == ""
    ):
        raise ValueError(f"Username and password must be provided!")

    try:
        return CyverseAccessToken.get(username, password)
    except:
        logger.error(f"Token request failed: {traceback.format_exc()}")
        raise


def user_info(username: str, token: str = None, timeout: int = 15):
    if token is not None:
        client = CyverseClient(token, timeout_seconds=timeout)
    else:
        raise ValueError(
            f"An authentication token must be explicitly provided for now"
        )
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        return client.user_info(username)
    except:
        logger.error(f"User profile request failed: {traceback.format_exc()}")
        raise


def paged_directory(path: str, token: str = None, timeout: int = 15):
    if token is not None:
        client = CyverseClient(token, timeout_seconds=timeout)
    else:
        raise ValueError(
            f"An authentication token must be explicitly provided for now"
        )
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        return client.list(path)
    except:
        logger.error(f"Failed to list files: {traceback.format_exc()}")
        raise


def stat(path: str, token: str = None, timeout: int = 15):
    if token is not None:
        client = CyverseClient(token, timeout_seconds=timeout)
    else:
        raise ValueError(
            f"An authentication token must be explicitly provided for now"
        )
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        return client.stat(path)
    except:
        logger.error(f"Failed to stat path: {traceback.format_exc()}")
        raise


def exists(path: str, type: str = None, token: str = None, timeout: int = 15):
    if token is not None:
        client = CyverseClient(token, timeout_seconds=timeout)
    else:
        raise ValueError(
            f"An authentication token must be explicitly provided for now"
        )
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        if type is None:
            return client.exists(path)
        if type == "dir":
            return client.dir_exists(path)
        elif type == "file":
            return client.file_exists(path)
        else:
            raise ValueError(f"Unsupported type (must be 'dir' or 'file')")
    except:
        logger.error(f"Failed to stat path: {traceback.format_exc()}")
        raise


def create(path: str, token: str = None, timeout: int = 15):
    if token is not None:
        client = CyverseClient(token, timeout_seconds=timeout)
    else:
        raise ValueError(
            f"An authentication token must be explicitly provided for now"
        )
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        return client.mkdir(path)
    except:
        logger.error(f"Failed to create directory: {traceback.format_exc()}")
        raise


def share(
    username: str,
    path: str,
    permission: str,
    token: str = None,
    timeout: int = 15,
):
    if token is not None:
        client = CyverseClient(token, timeout_seconds=timeout)
    else:
        raise ValueError(
            f"An authentication token must be explicitly provided for now"
        )
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        return client.share(username, path, permission)
    except:
        logger.error(
            f"Failed to share path {path} with user {username}: {traceback.format_exc()}"
        )
        raise


def unshare(username: str, path: str, token: str = None, timeout: int = 15):
    if token is not None:
        client = CyverseClient(token, timeout_seconds=timeout)
    else:
        raise ValueError(
            f"An authentication token must be explicitly provided for now"
        )
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        return client.unshare(username, path)
    except:
        logger.error(
            f"Failed to unshare path {path} from user {username}: {traceback.format_exc()}"
        )
        raise


def download(
    remote_path: str,
    local_path: str = None,
    patterns: List[str] = None,
    checksums: List[dict] = None,
    force: bool = False,
    token: str = None,
    timeout: int = 15,
):
    if token is not None:
        client = CyverseClient(token, timeout_seconds=timeout)
    else:
        raise ValueError(
            f"An authentication token must be explicitly provided for now"
        )
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        local_path = (
            getcwd()
            if (local_path is None or local_path == "")
            else local_path
        )
        Path(local_path).mkdir(exist_ok=True)

        if client.dir_exists(remote_path):
            client.download_directory(
                from_path=remote_path,
                to_path=local_path,
                patterns=patterns,
                checksums=checksums,
                overwrite=force,
            )
        elif client.file_exists(remote_path):
            client.download(
                from_path=remote_path, to_path=local_path, overwrite=force
            )
        else:
            msg = f"Path does not exist: {remote_path}"
            logger.error(msg)
            raise ValueError(msg)

        files = listdir(local_path)
        if len(files) == 0:
            msg = (
                f"No files found at path '{remote_path}'"
                + f" matching patterns {patterns}"
            )
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
    token: str = None,
    timeout: int = 15,
):
    if token is not None:
        client = CyverseClient(token, timeout_seconds=timeout)
    else:
        raise ValueError(
            f"An authentication token must be explicitly provided for now"
        )
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        client.upload_directory(
            local_path,
            remote_path,
            include_patterns,
            include_names,
            exclude_patterns,
            exclude_names,
        )
        logger.info(f"Pushed output(s)")
    except:
        logger.error(f"Failed to push outputs: {traceback.format_exc()}")
        raise


def tag(
    id: str,
    attributes: List[str],
    irods_attributes: List[str],
    token: str = None,
    timeout: int = 15,
):
    if token is not None:
        client = CyverseClient(token, timeout_seconds=timeout)
    else:
        raise ValueError(
            f"An authentication token must be explicitly provided for now"
        )
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        client.set_metadata(id, attributes, irods_attributes)
        logger.info(f"Configured metadata for data object with ID {id}")
    except:
        logger.error(
            f"Failed to configure metadata for data object with ID {id}: {traceback.format_exc()}"
        )
        raise


def tags(id: str, irods: bool, token: str = None, timeout: int = 15):
    if token is not None:
        client = CyverseClient(token, timeout_seconds=timeout)
    else:
        raise ValueError(
            f"An authentication token must be explicitly provided for now"
        )
        # TODO: try to load token from cache
        # client = TerrainClient(token)

    try:
        metadata = client.get_metadata(id, irods)
        logger.info(
            f"Retrieved {'iRODS' if irods else 'standard'} metadata for data object with ID {id}"
        )
        return [str(a["attr"] + "=" + a["value"]) for a in metadata]
    except:
        logger.error(
            f"Failed to retrieve {'iRODS' if irods else 'standard'} metadata for data object with ID {id}: {traceback.format_exc()}"
        )
        raise
