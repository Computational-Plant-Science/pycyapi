import os
import subprocess
from os.path import join, getsize, isdir
from pathlib import Path
from typing import List
from zipfile import ZIP_DEFLATED, ZipFile

from plantit_cli.store.store import Store
from plantit_cli.store.terrain_store import TerrainStore
from plantit_cli.utils import list_files


def clone(repo: str, branch: str = None):
    repo_name = repo.rpartition('/')[2]
    if isdir(repo_name):
        if subprocess.run(f"cd {repo_name} && git pull", stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).returncode != 0:
            raise ValueError(f"Failed to update repo {repo}")
        else:
            print(f"Updated repo '{repo}'")
    elif subprocess.run(f"git clone {repo}",
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        shell=True).returncode != 0:
        raise ValueError(f"Failed to clone repo {repo}")
    else:
        print(f"Cloned repo {repo}")

    if branch is not None:
        if subprocess.run(f"cd {repo_name} && git checkout {branch}", stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True).returncode != 0:
            raise ValueError(f"Failed to switch to branch {branch}")


def pull(store: Store, from_path: str, to_path: str, patterns: List[str] = None):
    Path(to_path).mkdir(exist_ok=True)

    if store.directory_exists(from_path):
        store.download_directory(from_path, to_path, patterns)
    elif store.file_exists(from_path):
        store.download_file(from_path, to_path)
    else:
        raise ValueError(f"Path does not exist in {type(store).__name__} store: {from_path}")

    files = os.listdir(to_path)
    if len(files) == 0:
        raise ValueError(f"No files found at path '{from_path}'" + f" matching patterns '{patterns}'")

    print(f"Pulled input(s): {', '.join(files)}")
    return to_path


def zip(
        path: str,
        name: str,
        max_size: int = 1000000000,  # 1GB default
        include_patterns: List[str] = None,
        include_names: List[str] = None,
        exclude_patterns: List[str] = None,
        exclude_names: List[str] = None):
    with ZipFile(join(path, f"{name}.zip"), 'w', ZIP_DEFLATED) as zip:
        files = list_files(path, include_patterns, include_names, exclude_patterns, exclude_names)
        sizes = [getsize(file) for file in files]
        total = sum(sizes)

        if total > max_size:
            print(f"Cumulative filesize ({total}) exceeds maximum ({max_size})")

        for file in files:
            print(f"Zipping: {file}")
            zip.write(file)


def push(store: TerrainStore,
         from_path: str,
         to_path: str,
         include_patterns: List[str] = None,
         include_names: List[str] = None,
         exclude_patterns: List[str] = None,
         exclude_names: List[str] = None):
    store.upload_directory(from_path, to_path, include_patterns, include_names, exclude_patterns, exclude_names)
    print(f"Pushed output(s)")


def run(
        input_kind: str,  # either 'file', 'files', or 'directory'
):
    pass
