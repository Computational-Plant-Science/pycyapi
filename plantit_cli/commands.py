import os
import traceback
from copy import deepcopy
from os.path import join, getsize, basename
from pathlib import Path
from typing import List
from zipfile import ZIP_DEFLATED, ZipFile

from dask_jobqueue import SLURMCluster, JobQueueCluster
from distributed import as_completed, LocalCluster, Client

from plantit_cli.options import FileChecksum, RunOptions, DirectoryInput, FilesInput, FileInput, Parameter
from plantit_cli.store.terrain_store import TerrainStore
from plantit_cli.utils import list_files, readable_bytes, prep_command, update_status, submit_command


def zip(
        input_dir: str,
        output_dir: str,
        name: str,
        max_size: int = 1000000000,  # 1GB default
        include_patterns: List[str] = None,
        include_names: List[str] = None,
        exclude_patterns: List[str] = None,
        exclude_names: List[str] = None,
        plantit_url: str = None,
        plantit_token: str = None):
    zip_path = join(output_dir, f"{name}.zip")
    try:
        files = list_files(input_dir, include_patterns, include_names, exclude_patterns, exclude_names)
        sizes = [getsize(file) for file in files]
        total = sum(sizes)

        if total > max_size:
            msg = f"Cumulative filesize ({readable_bytes(total)}) exceeds maximum ({readable_bytes(max_size)})"
            update_status(2, msg, plantit_url, plantit_token)
            raise ValueError(msg)

        update_status(3, f"Zipping {readable_bytes(total)} into file: {zip_path}", plantit_url, plantit_token)
        with ZipFile(zip_path, 'w', ZIP_DEFLATED) as zipped:
            for file in files:
                print(f"Zipping: {file}", plantit_url, plantit_token)
                zipped.write(file, basename(file))
    except:
        update_status(2, f"Failed to create zip file: {traceback.format_exc()}", plantit_url, plantit_token)
        raise


def pull(
        remote_path: str,
        local_path: str,
        cyverse_token: str = None,
        patterns: List[str] = None,
        checksums: List[FileChecksum] = None,
        overwrite: bool = False,
        plantit_url: str = None,
        plantit_token: str = None):
    try:
        store = TerrainStore(cyverse_token)
        Path(local_path).mkdir(exist_ok=True)

        if store.dir_exists(remote_path):
            store.pull_dir(from_path=remote_path, to_path=local_path, patterns=patterns, checksums=checksums, overwrite=overwrite)
        elif store.file_exists(remote_path):
            store.pull_file(from_path=remote_path, to_path=local_path, overwrite=overwrite)
        else:
            msg = f"Path does not exist in {type(store).__name__} store: {remote_path}"
            update_status(2, msg, plantit_url, plantit_token)
            raise ValueError(msg)

        files = os.listdir(local_path)
        if len(files) == 0:
            msg = f"No files found at path '{remote_path}'" + f" matching patterns '{patterns}'"
            update_status(2, msg, plantit_url, plantit_token)
            raise ValueError(msg)

        update_status(3, f"Pulled input(s): {', '.join(files)}", plantit_url, plantit_token)
        return local_path
    except:
        update_status(2, f"Failed to pull inputs: {traceback.format_exc()}", plantit_url, plantit_token)
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
        update_status(3, f"Pushed output(s)", plantit_url, plantit_token)
    except:
        update_status(2, f"Failed to push outputs: {traceback.format_exc()}", plantit_url, plantit_token)
        raise


def run(options: RunOptions,
        plantit_url: str = None,
        plantit_token: str = None,
        docker_username: str = None,
        docker_password: str = None):
    try:
        update_status(3, f"Running flow with definition:\n{options.to_json()}", plantit_url, plantit_token)
        cluster: JobQueueCluster = LocalCluster() if options.cluster is None else SLURMCluster(
                project=options.cluster['project'],
                queue=options.cluster['queue'],
                cores=options.cluster['cores'],
                memory=options.cluster['memory'],
                processes=options.cluster['processes'],
                walltime=options.cluster['walltime'],
                header_skip=options.cluster['header_skip'],
                extra=options.cluster['extra'])

        if options.input is None:
            with Client(cluster) as client:
                command = prep_command(
                    work_dir=options.workdir,
                    image=options.image,
                    command=options.command,
                    parameters=options.parameters if options.parameters is not None else [],
                    bind_mounts=options.bind_mounts,
                    docker_username=docker_username,
                    docker_password=docker_password)

                future = submit_command(client, command, options.log_file, 3)
                update_status(3, f"Submitted container", plantit_url, plantit_token)
                future.result()
                update_status(3, f"Container completed")
        elif isinstance(options.input, DirectoryInput):
            with Client(cluster) as client:
                command = prep_command(
                        work_dir=options.workdir,
                        image=options.image,
                        command=options.command,
                        parameters=(options.parameters if options.parameters is not None else []) + [Parameter(key='INPUT', value=options.input.path)],
                        bind_mounts=options.bind_mounts,
                        docker_username=docker_username,
                        docker_password=docker_password)

                future = submit_command(client, command, options.log_file, 3)
                update_status(3, f"Submitted container for directory: {options.input.path}", plantit_url, plantit_token)
                future.result()
                update_status(3, f"Container completed for directory: {options.input.path}")
        elif isinstance(options.input, FilesInput):
            files = os.listdir(options.input.path)
            count = len(files)
            futures = []

            if options.cluster is None:
                update_status(3, f"Processing {count} files in '{options.input.path}'")
            else:
                update_status(3, f"Requesting nodes to process {count} files in '{options.input.path}' with job script: {cluster.job_script()}")
                cluster.scale(count)

            with Client(cluster) as client:
                for file in files:
                    command = prep_command(
                            work_dir=options.workdir,
                            image=options.image,
                            command=options.command,
                            parameters=(deepcopy(options.parameters) if options.parameters else []) + \
                                       [Parameter(key='INPUT', value=join(options.input.path, file))],
                            bind_mounts=options.bind_mounts,
                            docker_username=docker_username,
                            docker_password=docker_password)

                    futures.append(submit_command(client, command, options.log_file, 3))
                    update_status(3, f"Submitted container for file: {file}", plantit_url, plantit_token)

                finished = 0
                for _ in as_completed(futures):
                    finished += 1
                    update_status(3, f"Container completed for file {finished} of {len(futures)}")
        elif isinstance(options.input, FileInput):
            with Client(cluster) as client:
                command = prep_command(
                    work_dir=options.workdir,
                    image=options.image,
                    command=options.command,
                    parameters=options.parameters if options.parameters else [] + \
                               [Parameter(key='INPUT', value=options.input.path)],
                    bind_mounts=options.bind_mounts,
                    docker_username=docker_username,
                    docker_password=docker_password)

                future = submit_command(client, command, options.log_file, 3)
                update_status(3, f"Submitted container for file: {options.input.path}", plantit_url, plantit_token)
                future.result()
                update_status(3, f"Container completed for file: {options.input.path}")

        update_status(3, f"Run completed", plantit_url, plantit_token)
    except:
        update_status(2, f"Run failed: {traceback.format_exc()}", plantit_url, plantit_token)
        raise
