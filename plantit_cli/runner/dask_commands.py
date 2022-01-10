import os
import traceback
from copy import deepcopy
from os.path import join
from pprint import pprint

import tqdm
from dask_jobqueue import SLURMCluster, PBSCluster, MoabCluster, SGECluster, LSFCluster, OARCluster
from distributed import LocalCluster, Client, as_completed

from plantit_cli.commands import logger
from plantit_cli.options import InputKind
from plantit_cli.utils import prep_command, submit_command


def run_dask(options: dict,
        docker_username: str = None,
        docker_password: str = None,
        docker: bool = False,
        slurm_job_array: bool = False):
    try:
        if 'jobqueue' not in options:
            cluster = LocalCluster()
        else:
            jobqueue = options['jobqueue']
            gpus = options['gpus'] if 'gpus' in options else 0
            if 'slurm' in jobqueue:
                print("Requesting SLURM cluster:")
                pprint(jobqueue['slurm'])
                cluster = SLURMCluster(job_extra=[f"--gres=gpu:{gpus}"], **jobqueue['slurm']) if gpus else SLURMCluster(**jobqueue['slurm'])
            elif 'pbs' in jobqueue:
                print("Requesting PBS cluster:")
                pprint(jobqueue['pbs'])
                cluster = PBSCluster(job_extra=[f"--gres=gpu:{gpus}"], **jobqueue['pbs']) if gpus else PBSCluster(**jobqueue['pbs'])
            elif 'moab' in jobqueue:
                print("Requesting MOAB cluster:")
                pprint(jobqueue['moab'])
                cluster = MoabCluster(job_extra=[f"--gres=gpu:{gpus}"], **jobqueue['moab']) if gpus else MoabCluster(**jobqueue['moab'])
            elif 'sge' in jobqueue:
                print("Requesting SGE cluster:")
                pprint(jobqueue['sge'])
                cluster = SGECluster(job_extra=[f"--gres=gpu:{gpus}"], **jobqueue['sge']) if gpus else SGECluster(**jobqueue['sge'])
            elif 'lsf' in jobqueue:
                print("Requesting LSF cluster:")
                pprint(jobqueue['lsf'])
                cluster = LSFCluster(job_extra=[f"--gres=gpu:{gpus}"], **jobqueue['lsf']) if gpus else LSFCluster(**jobqueue['lsf'])
            elif 'oar' in jobqueue:
                print("Requesting OAR cluster:")
                pprint(jobqueue['oar'])
                cluster = OARCluster(job_extra=[f"--gres=gpu:{gpus}"], **jobqueue['oar']) if gpus else OARCluster(**jobqueue['oar'])
            else:
                raise ValueError(f"Unsupported jobqueue configuration: {jobqueue}")

            print(f"Cluster job script: {cluster.job_script()}")

        if 'output' in options and 'from' in options['output']: output_path = options['output']['from']
        else: output_path = '.'

        if 'input' not in options:
            env = options['env'] if 'env' in options else []
            params = options['parameters'] if 'parameters' in options else []
            bind_mounts = options['bind_mounts'] if 'bind_mounts' in options else []
            no_cache = options['no_cache'] if 'no_cache' in options else False
            gpus = options['gpus'] if 'gpus' in options else 0

            if 'jobqueue' in options: cluster.scale(1)
            with Client(cluster) as client:
                command = prep_command(
                    work_dir=options['workdir'],
                    image=options['image'],
                    command=options['command'],
                    env=env + [{'key': 'INDEX', 'value': 1}],
                    parameters=params + [{'key': 'OUTPUT', 'value': output_path}],
                    bind_mounts=bind_mounts,
                    no_cache=no_cache,
                    gpus=gpus,
                    docker_username=docker_username,
                    docker_password=docker_password,
                    docker=docker)

                logger.info(f"Submitting container")
                future = submit_command(client, command, options['log_file'] if 'log_file' in options else None, 3)
                future.result()
                if future.status != 'finished':
                    logger.error(f"Container failed: {future.exception}")
                else:
                    logger.info(f"Container completed")
        elif options['input']['kind'] == InputKind.DIRECTORY:
            input_path = options['input']['path']
            env = options['env'] if 'env' in options else []
            params = options['parameters'] if 'parameters' in options else []
            bind_mounts = options['bind_mounts'] if 'bind_mounts' in options else []
            no_cache = options['no_cache'] if 'no_cache' in options else False
            gpus = options['gpus'] if 'gpus' in options else 0

            if 'jobqueue' in options: cluster.scale(1)
            with Client(cluster) as client:
                command = prep_command(
                    work_dir=options['workdir'],
                    image=options['image'],
                    command=options['command'],
                    env=env + [{'key': 'INDEX', 'value': 1}],
                    parameters=params + [{'key': 'INPUT', 'value': input_path}, {'key': 'OUTPUT', 'value': output_path}],
                    bind_mounts=bind_mounts,
                    no_cache=no_cache,
                    gpus=gpus,
                    docker_username=docker_username,
                    docker_password=docker_password,
                    docker=docker)

                logger.info(f"Submitting container for directory '{input_path}'")
                future = submit_command(client, command, options['log_file'] if 'log_file' in options else None, 3)
                future.result()
                if future.status != 'finished':
                    logger.error(f"Container failed for directory '{input_path}': {future.exception}")
                else:
                    logger.info(f"Container completed for directory '{input_path}'")
        elif options['input']['kind'] == InputKind.FILES:
            input_path = options['input']['path']
            if slurm_job_array:
                files = os.listdir(input_path)
                file_id = int(os.environ.get('SLURM_ARRAY_TASK_ID'))
                current_file = files[file_id]

                env = options['env'] if 'env' in options else []
                params = options['parameters'] if 'parameters' in options else []
                patterns = options['input']['patterns'] if 'patterns' in options['input'] else []
                bind_mounts = options['bind_mounts'] if 'bind_mounts' in options else []
                no_cache = options['no_cache'] if 'no_cache' in options else False
                gpus = options['gpus'] if 'gpus' in options else 0

                if 'jobqueue' in options: cluster.scale(1)
                with Client(cluster) as client:
                    command = prep_command(
                        work_dir=options['workdir'],
                        image=options['image'],
                        command=options['command'],
                        env=env + [{'key': 'INDEX', 'value': file_id}] + [{'key': 'PATTERNS', 'value': ','.join(patterns)}],
                        parameters=params + [{'key': 'INPUT', 'value': join(input_path, current_file)}, {'key': 'OUTPUT', 'value': output_path}],
                        bind_mounts=bind_mounts,
                        no_cache=no_cache,
                        gpus=gpus,
                        docker_username=docker_username,
                        docker_password=docker_password,
                        docker=docker)

                    logger.info(f"Submitting container for file '{input_path}'")
                    future = submit_command(client, command, options['log_file'] if 'log_file' in options else None, 3)
                    future.result()
                    if future.status != 'finished':
                        logger.error(f"Container failed for file '{input_path}': {future.exception}")
                    else:
                        logger.info(f"Container completed for file '{input_path}'")

                logger.info(f"Run succeeded")
            else:
                files = os.listdir(input_path)
                count = len(files)
                futures = []

                if 'jobqueue' not in options:
                    logger.info(f"Processing {count} files in '{input_path}'")
                else:
                    logger.info(f"Requesting {count} nodes to process {count} files in '{input_path}' with job script:\n{cluster.job_script()}")
                    cluster.scale(count)

                env = options['env'] if 'env' in options else []
                params = deepcopy(options['parameters']) if 'parameters' in options else []
                patterns = options['input']['patterns'] if 'patterns' in options['input'] else []
                bind_mounts = options['bind_mounts'] if 'bind_mounts' in options else []
                no_cache = options['no_cache'] if 'no_cache' in options else False
                gpus = options['gpus'] if 'gpus' in options else 0

                with Client(cluster) as client:
                    num_files = len(files)
                    for i, current_file in tqdm.tqdm(enumerate(files), total=num_files):
                        command = prep_command(
                            work_dir=options['workdir'],
                            image=options['image'],
                            command=options['command'],
                            env=env + [{'key': 'INDEX', 'value': i}] + [{'key': 'PATTERNS', 'value': ','.join(patterns)}],
                            parameters=params + [{'key': 'INPUT', 'value': join(input_path, current_file)}, {'key': 'OUTPUT', 'value': output_path}],
                            bind_mounts=bind_mounts,
                            no_cache=no_cache,
                            gpus=gpus,
                            docker_username=docker_username,
                            docker_password=docker_password,
                            docker=docker)

                        logger.info(f"Submitting container for file {i}")
                        futures.append(submit_command(client, command, options['log_file'] if 'log_file' in options else None, 3))

                    finished = 0
                    for future in tqdm.tqdm(as_completed(futures), total=num_files):
                        finished += 1
                        if future.status != 'finished':
                            logger.error(f"Container failed for file {finished}: {future.exception}")
                        else:
                            logger.info(f"Container completed for file {finished}")
        elif options['input']['kind'] == InputKind.FILE:
            input_path = options['input']['path']
            env = options['env'] if 'env' in options else []
            params = options['parameters'] if 'parameters' in options else []
            patterns = options['input']['patterns'] if 'patterns' in options['input'] else []
            bind_mounts = options['bind_mounts'] if 'bind_mounts' in options else []
            no_cache = options['no_cache'] if 'no_cache' in options else False
            gpus = options['gpus'] if 'gpus' in options else 0

            if 'jobqueue' in options: cluster.scale(1)
            with Client(cluster) as client:
                command = prep_command(
                    work_dir=options['workdir'],
                    image=options['image'],
                    command=options['command'],
                    env=env + [{'key': 'INDEX', 'value': 1}] + [{'key': 'PATTERNS', 'value': ','.join(patterns)}],
                    parameters=params + [{'key': 'INPUT', 'value': input_path}, {'key': 'OUTPUT', 'value': output_path}],
                    bind_mounts=bind_mounts,
                    no_cache=no_cache,
                    gpus=gpus,
                    docker_username=docker_username,
                    docker_password=docker_password,
                    docker=docker)

                logger.info(f"Submitting container for file 1")
                future = submit_command(client, command, options['log_file'] if 'log_file' in options else None, 3)
                future.result()
                if future.status != 'finished':
                    logger.error(f"Container failed for file 1")
                    logger.error(future.exception)
                else:
                    logger.info(f"Container completed for file 1")

        logger.info(f"Run succeeded")
    except:
        logger.error(f"Run failed: {traceback.format_exc()}")
        raise