import logging
import traceback
from os import listdir, getcwd
from pathlib import Path
from typing import List

from plantit_cli.store import terrain_store

logger = logging.getLogger(__name__)


def pull(
        token: str,
        remote_path: str,
        local_path: str = None,
        patterns: List[str] = None,
        checksums: List[dict] = None,
        overwrite: bool = False):
    try:
        local_path = getcwd() if (local_path is None or local_path == '') else local_path
        Path(local_path).mkdir(exist_ok=True)

        if terrain_store.dir_exists(remote_path, token):
            terrain_store.pull_dir(from_path=remote_path, to_path=local_path, token=token, patterns=patterns, checksums=checksums,
                                   overwrite=overwrite)
        elif terrain_store.file_exists(remote_path, token):
            terrain_store.pull_file(from_path=remote_path, to_path=local_path, token=token, overwrite=overwrite)
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


def push(
        token: str,
        local_path: str,
        remote_path: str,
        include_patterns: List[str] = None,
        include_names: List[str] = None,
        exclude_patterns: List[str] = None,
        exclude_names: List[str] = None):
    try:
        terrain_store.push_dir(local_path, remote_path, token, include_patterns, include_names, exclude_patterns, exclude_names)
        logger.info(f"Pushed output(s)")
    except:
        logger.error(f"Failed to push outputs: {traceback.format_exc()}")
        raise
