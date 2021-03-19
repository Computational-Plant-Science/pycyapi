import traceback
from os import listdir, getcwd
from pathlib import Path
from typing import List

from plantit_cli.options import FileChecksum
from plantit_cli.status import Status
from plantit_cli.store.terrain_store import TerrainStore
from plantit_cli.utils import update_status


def pull(
        remote_path: str,
        local_path: str = None,
        cyverse_token: str = None,
        patterns: List[str] = None,
        checksums: List[FileChecksum] = None,
        overwrite: bool = False,
        plantit_url: str = None,
        plantit_token: str = None):
    try:
        store = TerrainStore(cyverse_token)
        local_path = getcwd() if local_path is None else local_path
        Path(local_path).mkdir(exist_ok=True)

        if store.dir_exists(remote_path):
            store.pull_dir(from_path=remote_path, to_path=local_path, patterns=patterns if patterns is not None else None, checksums=checksums, overwrite=overwrite)
        elif store.file_exists(remote_path):
            store.pull_file(from_path=remote_path, to_path=local_path, overwrite=overwrite)
        else:
            msg = f"Path does not exist in {type(store).__name__} store: {remote_path}"
            update_status(Status.FAILED, msg, plantit_url, plantit_token)
            raise ValueError(msg)

        files = listdir(local_path)
        if len(files) == 0:
            msg = f"No files found at path '{remote_path}'" + f" matching patterns '{patterns}'"
            update_status(Status.FAILED, msg, plantit_url, plantit_token)
            raise ValueError(msg)
        update_status(Status.PULLING, f"Pulled input(s): {', '.join(files)}", plantit_url, plantit_token)
        return local_path
    except:
        update_status(Status.FAILED, f"Failed to pull inputs: {traceback.format_exc()}", plantit_url, plantit_token)
        raise


def push(local_path: str,
         remote_path: str,
         cyverse_token: str,
         include_patterns: List[str] = None,
         include_names: List[str] = None,
         exclude_patterns: List[str] = None,
         exclude_names: List[str] = None,
         plantit_url: str = None,
         plantit_token: str = None):
    try:
        store = TerrainStore(cyverse_token)
        store.push_dir(local_path, remote_path, include_patterns, include_names, exclude_patterns, exclude_names)
        update_status(Status.PUSHING, f"Pushed output(s)", plantit_url, plantit_token)
    except:
        update_status(Status.FAILED, f"Failed to push outputs: {traceback.format_exc()}", plantit_url, plantit_token)
        raise