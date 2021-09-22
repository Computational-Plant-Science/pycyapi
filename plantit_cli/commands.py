import os
import traceback
from copy import deepcopy
from os.path import join, getsize, basename
from pprint import pprint
from typing import List
from zipfile import ZIP_DEFLATED, ZipFile

from dask_jobqueue import SLURMCluster, PBSCluster, MoabCluster, SGECluster, LSFCluster, OARCluster
from distributed import as_completed, LocalCluster, Client
import tqdm

from plantit_cli.options import InputKind
from plantit_cli.status import Status
from plantit_cli.utils import list_files, readable_bytes, prep_command, update_status, submit_command, run_command, replace_text

message = "Message"
testdir = os.environ.get('TEST_DIRECTORY')


def clean(paths: List[str], patterns: List[str]):
    for path in paths:
        for pattern in patterns:
            replace_text(path, pattern, ''.join(['*' for _ in pattern]))


def run(options: dict,
        plantit_url: str = None,
        plantit_token: str = None,
        docker_username: str = None,
        docker_password: str = None,
        slurm_job_array: bool = False):
    try:
        if 'jobqueue' not in options:
            cluster = LocalCluster()
        else:
            jobqueue = options['jobqueue']
            if 'slurm' in jobqueue:
                print("Requesting SLURM cluster:")
                pprint(jobqueue['slurm'])
                cluster = SLURMCluster(**jobqueue['slurm'])
            elif 'pbs' in jobqueue:
                print("Requesting PBS cluster:")
                pprint(jobqueue['pbs'])
                cluster = PBSCluster(**jobqueue['pbs'])
            elif 'moab' in jobqueue:
                print("Requesting MOAB cluster:")
                pprint(jobqueue['moab'])
                cluster = MoabCluster(**jobqueue['moab'])
            elif 'sge' in jobqueue:
                print("Requesting SGE cluster:")
                cluster = SGECluster(**jobqueue['sge'])
                pprint(jobqueue['sge'])
            elif 'lsf' in jobqueue:
                print("Requesting LSF cluster:")
                cluster = LSFCluster(**jobqueue['lsf'])
                pprint(jobqueue['lsf'])
            elif 'oar' in jobqueue:
                print("Requesting OAR cluster:")
                pprint(jobqueue['oar'])
                cluster = OARCluster(**jobqueue['oar'])
            else:
                raise ValueError(f"Unsupported jobqueue configuration: {jobqueue}")

        if 'input' not in options:
            env = options['env'] if 'env' in options else []
            params = options['parameters'] if 'parameters' in options else []
            bind_mounts = options['bind_mounts'] if 'bind_mounts' in options else []
            no_cache = options['no_cache'] if 'no_cache' in options else False
            gpu = options['gpu'] if 'gpu' in options else False

            if 'jobqueue' not in options: cluster.scale(1)
            with Client(cluster) as client:
                command = prep_command(
                    work_dir=options['workdir'],
                    image=options['image'],
                    command=options['command'],
                    env=env + [{'key': 'INDEX', 'value': 1}],
                    parameters=params,
                    bind_mounts=bind_mounts,
                    no_cache=no_cache,
                    gpu=gpu,
                    docker_username=docker_username,
                    docker_password=docker_password)

                update_status(Status.RUNNING, f"Submitting container", plantit_url, plantit_token)
                future = submit_command(client, command, options['log_file'] if 'log_file' in options else None, 3)
                future.result()
                if future.status != 'finished':
                    update_status(Status.FAILED, f"Container failed: {future.exception}", plantit_url, plantit_token)
                else:
                    update_status(Status.RUNNING, f"Container completed", plantit_url, plantit_token)
        elif options['input']['kind'] == InputKind.DIRECTORY:
            input_path = options['input']['path']
            env = options['env'] if 'env' in options else []
            params = options['parameters'] if 'parameters' in options else []
            bind_mounts = options['bind_mounts'] if 'bind_mounts' in options else []
            no_cache = options['no_cache'] if 'no_cache' in options else False
            gpu = options['gpu'] if 'gpu' in options else False

            if 'jobqueue' in options: cluster.scale(1)
            with Client(cluster) as client:
                command = prep_command(
                    work_dir=options['workdir'],
                    image=options['image'],
                    command=options['command'],
                    env=env + [{'key': 'INDEX', 'value': 1}],
                    parameters=params + [{'key': 'INPUT', 'value': input_path}],
                    bind_mounts=bind_mounts,
                    no_cache=no_cache,
                    gpu=gpu,
                    docker_username=docker_username,
                    docker_password=docker_password)

                update_status(Status.RUNNING, f"Submitting container for directory '{input_path}'", plantit_url, plantit_token)
                future = submit_command(client, command, options['log_file'] if 'log_file' in options else None, 3)
                future.result()
                if future.status != 'finished':
                    update_status(Status.FAILED, f"Container failed for directory '{input_path}': {future.exception}", plantit_url, plantit_token)
                else:
                    update_status(Status.RUNNING, f"Container completed for directory '{input_path}'", plantit_url, plantit_token)
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
                gpu = options['gpu'] if 'gpu' in options else False

                if 'jobqueue' in options: cluster.scale(1)
                with Client(cluster) as client:
                    command = prep_command(
                        work_dir=options['workdir'],
                        image=options['image'],
                        command=options['command'],
                        env=env + [{'key': 'INDEX', 'value': file_id}] + [{'key': 'PATTERNS', 'value': ','.join(patterns)}],
                        parameters=params + [{'key': 'INPUT', 'value': join(input_path, current_file)}],
                        bind_mounts=bind_mounts,
                        no_cache=no_cache,
                        gpu=gpu,
                        docker_username=docker_username,
                        docker_password=docker_password)

                    update_status(Status.RUNNING, f"Submitting container for file '{input_path}'", plantit_url, plantit_token)
                    future = submit_command(client, command, options['log_file'] if 'log_file' in options else None, 3)
                    future.result()
                    if future.status != 'finished':
                        update_status(Status.FAILED, f"Container failed for file '{input_path}': {future.exception}", plantit_url, plantit_token)
                    else:
                        update_status(Status.RUNNING, f"Container completed for file '{input_path}'", plantit_url, plantit_token)

                update_status(Status.COMPLETED, f"Run succeeded", plantit_url, plantit_token)
            else:
                files = os.listdir(input_path)
                count = len(files)
                futures = []

                if 'jobqueue' not in options:
                    update_status(Status.RUNNING, f"Processing {count} files in '{input_path}'", plantit_url, plantit_token)
                else:
                    update_status(Status.RUNNING,
                                  f"Requesting {count} nodes to process {count} files in '{input_path}' with job script:\n{cluster.job_script()}",
                                  plantit_url, plantit_token)
                    cluster.scale(count)

                env = options['env'] if 'env' in options else []
                params = deepcopy(options['parameters']) if 'parameters' in options else []
                patterns = options['input']['patterns'] if 'patterns' in options['input'] else []
                bind_mounts = options['bind_mounts'] if 'bind_mounts' in options else []
                no_cache = options['no_cache'] if 'no_cache' in options else False
                gpu = options['gpu'] if 'gpu' in options else False

                with Client(cluster) as client:
                    num_files = len(files)
                    for i, current_file in tqdm.tqdm(enumerate(files), total=num_files):
                        command = prep_command(
                            work_dir=options['workdir'],
                            image=options['image'],
                            command=options['command'],
                            env=env + [{'key': 'INDEX', 'value': i}] + [{'key': 'PATTERNS', 'value': ','.join(patterns)}],
                            parameters=params + [{'key': 'INPUT', 'value': join(input_path, current_file)}],
                            bind_mounts=bind_mounts,
                            no_cache=no_cache,
                            gpu=gpu,
                            docker_username=docker_username,
                            docker_password=docker_password)

                        update_status(Status.RUNNING, f"Submitting container for file {i}", plantit_url, plantit_token)
                        futures.append(submit_command(client, command, options['log_file'] if 'log_file' in options else None, 3))

                    finished = 0
                    for future in tqdm.tqdm(as_completed(futures), total=num_files):
                        finished += 1
                        if future.status != 'finished':
                            update_status(Status.FAILED, f"Container failed for file {finished}", plantit_url, plantit_token)
                            update_status(Status.FAILED, future.exception, plantit_url, plantit_token)
                        else:
                            update_status(Status.RUNNING, f"Container completed for file {finished}", plantit_url, plantit_token)
        elif options['input']['kind'] == InputKind.FILE:
            input_path = options['input']['path']
            env = options['env'] if 'env' in options else []
            params = options['parameters'] if 'parameters' in options else []
            patterns = options['input']['patterns'] if 'patterns' in options['input'] else []
            bind_mounts = options['bind_mounts'] if 'bind_mounts' in options else []
            no_cache = options['no_cache'] if 'no_cache' in options else False
            gpu = options['gpu'] if 'gpu' in options else False

            if 'jobqueue' in options: cluster.scale(1)
            with Client(cluster) as client:
                command = prep_command(
                    work_dir=options['workdir'],
                    image=options['image'],
                    command=options['command'],
                    env=env + [{'key': 'INDEX', 'value': 1}] + [{'key': 'PATTERNS', 'value': ','.join(patterns)}],
                    parameters=params + [{'key': 'INPUT', 'value': input_path}],
                    bind_mounts=bind_mounts,
                    no_cache=no_cache,
                    gpu=gpu,
                    docker_username=docker_username,
                    docker_password=docker_password)

                update_status(Status.RUNNING, f"Submitting container for file 1", plantit_url, plantit_token)
                future = submit_command(client, command, options['log_file'] if 'log_file' in options else None, 3)
                future.result()
                if future.status != 'finished':
                    update_status(Status.FAILED, f"Container failed for file 1", plantit_url, plantit_token)
                    update_status(Status.FAILED, future.exception, plantit_url, plantit_token)
                else:
                    update_status(Status.RUNNING, f"Container completed for file 1", plantit_url, plantit_token)

        update_status(Status.COMPLETED, f"Run succeeded", plantit_url, plantit_token)
    except:
        update_status(Status.FAILED, f"Run failed: {traceback.format_exc()}", plantit_url, plantit_token)
        raise


def zip(
        input_dir: str,
        output_dir: str,
        name: str,
        # max_size: int = 1000000000,  # 1GB default
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

        # if total > max_size:
        #     msg = f"Cumulative filesize ({readable_bytes(total)}) exceeds maximum ({readable_bytes(max_size)})"
        #     update_status(Status.ZIPPING, msg, plantit_url, plantit_token)
        #     raise ValueError(msg)

        update_status(Status.RUNNING, f"Zipping {readable_bytes(total)} into file: {zip_path}", plantit_url, plantit_token)
        with ZipFile(zip_path, 'w', ZIP_DEFLATED) as zipped:
            for file in files:
                print(f"Zipping: {file}", plantit_url, plantit_token)
                zipped.write(file, basename(file))
    except:
        update_status(Status.FAILED, f"Failed to create zip file: {traceback.format_exc()}", plantit_url, plantit_token)
        raise


# TODO test flow configuration validation

# @pytest.mark.skip(reason="y dis fail on CI VM")
# def test_run_logs_to_file_when_file_logging_enabled():
#     with TemporaryDirectory() as temp_dir:
#         log_file_name = 'test_run_logs_to_file_when_file_logging_enabled.txt'
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_params_and_no_input_and_no_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='echo "$MESSAGE" >> $MESSAGE_FILE',
#             parameters=[
#                 {
#                     'key': 'MESSAGE',
#                     'value': message
#                 },
#                 {
#                     'key': 'MESSAGE_FILE',
#                     'value': 'message.txt'
#                 },
#             ],
#             logging={
#                 'file': log_file_name
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#         try:
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check local file was written
#             file = join(testdir, 'message.txt')
#             assert isfile(file)
#             with open(file) as file:
#                 lines = file.readlines()
#                 assert len(lines) == 1
#                 assert lines[0] == f"{message}\n"
#
#             # check log file was written
#             log_file = join(testdir, log_file_name)
#             assert isfile(log_file)
#             with open(log_file) as log_file:
#                 assert len(log_file.readlines()) > 0
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_params_and_no_input_and_no_output():
#     with TemporaryDirectory() as temp_dir:
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_params_and_no_input_and_no_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='echo "$MESSAGE" >> $MESSAGE_FILE',
#             parameters=[
#                 {
#                     'key': 'MESSAGE',
#                     'value': message
#                 },
#                 {
#                     'key': 'MESSAGE_FILE',
#                     'value': join(testdir, 'message.txt')
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#         try:
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check local file was written
#             file = join(testdir, 'message.txt')
#             assert isfile(file)
#             with open(file) as file:
#                 lines = file.readlines()
#                 assert len(lines) == 1
#                 assert lines[0] == f"{message}\n"
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_no_params_and_file_input_and_no_output(remote_base_path, file_name_1):
#     with TemporaryDirectory() as temp_dir:
#         local_path = join(testdir, file_name_1)
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_no_params_and_file_input_and_no_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='cat "$INPUT" | tee "$INPUT.output"',
#             input={
#                 'kind': 'file',
#                 'from': join(remote_path, file_name_1),
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # prep file
#             with open(local_path, "w") as file1:
#                 file1.write('Hello, 1!')
#             store.push_file(local_path, remote_path)
#
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check file was pulled
#             downloaded_path = join(testdir, 'input', file_name_1)
#             check_hello(downloaded_path, 1)
#             remove(downloaded_path)
#
#             # check local output file was written
#             output_1 = f"{downloaded_path}.output"
#             check_hello(output_1, 1)
#             remove(output_1)
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_params_and_file_input_and_no_output(remote_base_path, file_name_1):
#     with TemporaryDirectory() as temp_dir:
#         local_path = join(testdir, file_name_1)
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_params_and_file_input_and_no_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='cat "$INPUT" | tee "$INPUT.$TAG.output"',
#             input={
#                 'kind': 'file',
#                 'from': join(remote_path, file_name_1),
#             },
#             parameters=[
#                 {
#                     'key': 'TAG',
#                     'value': message
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # prep file
#             with open(local_path, "w") as file1:
#                 file1.write('Hello, 1!')
#             store.push_file(local_path, remote_path)
#
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check file was pulled
#             downloaded_path = join(testdir, 'input', file_name_1)
#             check_hello(downloaded_path, 1)
#             remove(downloaded_path)
#
#             # check local output file was written
#             output_1 = f"{downloaded_path}.{message}.output"
#             check_hello(output_1, 1)
#             remove(output_1)
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_fails_with_no_params_and_file_input_and_no_output_when_no_inputs_found(remote_base_path, file_name_1):
#     with TemporaryDirectory() as temp_dir:
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_fails_with_no_params_and_file_input_and_no_output_when_no_inputs_found',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='cat "$INPUT" | tee "$INPUT.output"',
#             input={
#                 'kind': 'file',
#                 'from': join(remote_path, file_name_1),
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         # expect exception
#         with pytest.raises(ValueError):
#             Runner(store).run(plan)
#
#
# def test_run_fails_with_params_and_file_input_and_no_output_when_no_inputs_found(remote_base_path, file_name_1):
#     with TemporaryDirectory() as temp_dir:
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_fails_with_params_and_file_input_and_no_output_when_no_inputs_found',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='cat "$INPUT" | tee "$INPUT.$TAG.output"',
#             input={
#                 'kind': 'file',
#                 'from': join(remote_path, file_name_1),
#             },
#             parameters=[
#                 {
#                     'key': 'TAG',
#                     'value': message
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # expect exception
#             with pytest.raises(ValueError):
#                 Runner(store).run(plan)
#         finally:
#             clear_dir(testdir)
#
#
# # TODO move to run section of test_commands
# def test_run_succeeds_with_no_params_and_files_input_and_no_output(remote_base_path, file_name_1, file_name_2):
#     with TemporaryDirectory() as temp_dir:
#         local_path_1 = join(testdir, file_name_1)
#         local_path_2 = join(testdir, file_name_2)
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_no_params_and_files_input_and_no_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='cat $INPUT | tee $INPUT.output',
#             input={
#                 'kind': 'files',
#                 'from': remote_path,
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # prep files
#             with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
#                 file1.write('Hello, 1!')
#                 file2.write('Hello, 2!')
#             store.push_file(local_path_1, remote_path)
#             store.push_file(local_path_2, remote_path)
#
#             # expect 2 containers
#             Runner(store).run(plan)
#
#             # check files were pulled
#             downloaded_path_1 = join(testdir, 'input', file_name_1)
#             downloaded_path_2 = join(testdir, 'input', file_name_2)
#             check_hello(downloaded_path_1, 1)
#             check_hello(downloaded_path_2, 2)
#             remove(downloaded_path_1)
#             remove(downloaded_path_2)
#
#             # check local output files were written
#             output_1 = f"{downloaded_path_1}.output"
#             output_2 = f"{downloaded_path_2}.output"
#             check_hello(output_1, 1)
#             check_hello(output_2, 2)
#             remove(output_1)
#             remove(output_2)
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_fails_with_no_params_and_files_input_and_no_output_when_no_inputs_found(remote_base_path):
#     with TemporaryDirectory() as temp_dir:
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_fails_with_no_params_and_files_input_and_no_output_when_no_inputs_found',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='cat $INPUT | tee $INPUT.output',
#             input={
#                 'kind': 'files',
#                 'from': remote_path,
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         # expect exception
#         with pytest.raises(ValueError):
#             Runner(store).run(plan)
#
#
# def test_run_fails_with_params_and_files_input_and_no_output_when_no_inputs_found(remote_base_path):
#     with TemporaryDirectory() as temp_dir:
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_fails_with_params_and_files_input_and_no_output_when_no_inputs_found',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='cat $INPUT | tee $INPUT.$TAG.output',
#             input={
#                 'kind': 'files',
#                 'from': remote_path,
#             },
#             parameters=[
#                 {
#                     'key': 'TAG',
#                     'value': message
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         # expect exception
#         with pytest.raises(ValueError):
#             Runner(store).run(plan)
#
#
# @pytest.mark.skip(reason='until fixed in CI')
# def test_run_succeeds_with_no_params_and_no_input_and_file_output(remote_base_path):
#     with TemporaryDirectory() as temp_dir:
#         output_path = join(testdir, 'output.txt')
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_no_params_and_no_input_and_file_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='echo "Hello, world!" >> output.txt',
#             output={
#                 'to': remote_path,
#                 'include': {
#                     'names': ['output.txt']
#                 }
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check file was pushed
#             files = store.list_dir(remote_path)
#             assert join(store.dir, remote_path, 'output.txt') in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# @pytest.mark.skip(reason='until fixed in CI')
# def test_run_succeeds_with_params_and_no_input_and_file_output(remote_base_path):
#     with TemporaryDirectory() as temp_dir:
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_params_and_no_input_and_file_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='echo "Hello, world!" >> "$TAG".txt',
#             output={
#                 'to': remote_path,
#                 'include': {
#                     'patterns': ["txt"]
#                 }
#             },
#             parameters=[
#                 {
#                     'key': 'TAG',
#                     'value': message
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check file was pushed
#             files = store.list_dir(remote_path)
#             assert join(store.dir, remote_path, f"{message}.txt") in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_no_params_and_no_input_and_directory_output(remote_base_path):
#     with TemporaryDirectory() as temp_dir:
#         output_path = testdir
#         output_file_1 = join(output_path, 't1.txt')
#         output_file_2 = join(output_path, 't2.txt')
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='echo "Hello, world!" | tee $OUTPUT/t1.txt $OUTPUT/t2.txt',
#             output={
#                 'to': remote_path,
#                 'from': '',
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check files were written locally
#             assert isfile(output_file_1)
#             assert isfile(output_file_2)
#             check_hello(output_file_1, 'world')
#             check_hello(output_file_2, 'world')
#             remove(output_file_1)
#             remove(output_file_2)
#
#             # check files were pushed
#             files = store.list_dir(remote_path)
#             assert join(store.dir, remote_path, 't1.txt') in files
#             assert join(store.dir, remote_path, 't2.txt') in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_params_and_no_input_and_directory_output(remote_base_path):
#     with TemporaryDirectory() as temp_dir:
#         output_path = testdir
#         output_file_1 = join(output_path, f"t1.{message}.txt")
#         output_file_2 = join(output_path, f"t2.{message}.txt")
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_params_and_no_input_and_directory_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='echo "Hello, world!" | tee $OUTPUT/t1.$TAG.txt $OUTPUT/t2.$TAG.txt',
#             output={
#                 'to': remote_path,
#                 'from': '',
#             },
#             parameters=[
#                 {
#                     'key': 'TAG',
#                     'value': message
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check files were written locally
#             assert isfile(output_file_1)
#             assert isfile(output_file_2)
#             check_hello(output_file_1, 'world')
#             check_hello(output_file_2, 'world')
#             remove(output_file_1)
#             remove(output_file_2)
#
#             # check files were pushed
#             files = store.list_dir(remote_path)
#             assert join(store.dir, remote_path, f"t1.{message}.txt") in files
#             assert join(store.dir, remote_path, f"t2.{message}.txt") in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_no_params_and_file_input_and_directory_output(remote_base_path, file_name_1):
#     with TemporaryDirectory() as temp_dir:
#         input_file_path = join(testdir, file_name_1)
#         output_path = join(testdir, 'input')  # write output files to input dir
#         output_file_path = join(output_path, f"{file_name_1}.output")
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_no_params_and_file_input_and_directory_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='cat $INPUT | tee $INPUT.output',
#             input={
#                 'kind': 'file',
#                 'from': join(remote_path, file_name_1),
#             },
#             output={
#                 'to': remote_path,
#                 'from': 'input',  # write output files to input dir
#                 'include': {
#                     'patterns': ['output'],
#                     'names': []}
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # prep file
#             with open(input_file_path, "w") as file1:
#                 file1.write('Hello, 1!')
#             store.push_file(input_file_path, remote_path)
#
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check file was written locally
#             assert isfile(output_file_path)
#             check_hello(output_file_path, '1')
#             remove(output_file_path)
#
#             # check file was pushed to store
#             files = store.list_dir(remote_path)
#             assert join(store.dir, remote_path, f"{file_name_1}.output") in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_params_and_file_input_and_directory_output(remote_base_path, file_name_1):
#     with TemporaryDirectory() as temp_dir:
#         input_file_path = join(testdir, file_name_1)
#         output_path = join(testdir, 'input')  # write output files to input dir
#         output_file_path = join(output_path, f"{file_name_1}.{message}.output")
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_params_and_file_input_and_directory_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='cat $INPUT | tee $INPUT.$TAG.output',
#             input={
#                 'kind': 'file',
#                 'from': join(remote_path, file_name_1),
#             },
#             output={
#                 'to': remote_path,
#                 'from': 'input',  # write output files to input dir
#                 'include': {
#                     'patterns': ['output'],
#                     'names': []
#                 }
#             },
#             parameters=[
#                 {
#                     'key': 'TAG',
#                     'value': message
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # prep file
#             with open(input_file_path, "w") as file1:
#                 file1.write('Hello, 1!')
#             store.push_file(input_file_path, remote_path)
#
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check file was written locally
#             assert isfile(output_file_path)
#             check_hello(output_file_path, '1')
#             remove(output_file_path)
#
#             # check file was pushed to store
#             files = store.list_dir(remote_path)
#             assert join(store.dir, remote_path, f"{file_name_1}.{message}.output") in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_no_params_and_files_input_and_directory_output(remote_base_path,
#                                                                           file_name_1,
#                                                                           file_name_2):
#     with TemporaryDirectory() as temp_dir:
#         input_file_path_1 = join(testdir, file_name_1)
#         input_file_path_2 = join(testdir, file_name_2)
#         output_path = join(testdir, 'input')  # write output files to input dir
#         output_file_path_1 = join(output_path, f"{file_name_1}.output")
#         output_file_path_2 = join(output_path, f"{file_name_2}.output")
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_no_params_and_files_input_and_directory_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='cat $INPUT | tee $INPUT.output',
#             input={
#                 'kind': 'files',
#                 'from': remote_path,
#             },
#             output={
#                 'to': remote_path,
#                 'from': 'input',  # write output files to input dir
#                 'include': {
#                     'patterns': ['output'],
#                     'names': []
#                 }
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # prep file
#             with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
#                 file1.write('Hello, 1!')
#                 file2.write('Hello, 2!')
#             store.push_file(input_file_path_1, remote_path)
#             store.push_file(input_file_path_2, remote_path)
#
#             # expect 2 containers
#             Runner(store).run(plan)
#
#             # check files were written locally
#             assert isfile(output_file_path_1)
#             assert isfile(output_file_path_2)
#             check_hello(output_file_path_1, '1')
#             check_hello(output_file_path_2, '2')
#             remove(output_file_path_1)
#             remove(output_file_path_2)
#
#             # check file was pushed to store
#             files = store.list_dir(remote_path)
#             assert join(store.dir, remote_path, f"{file_name_1}.output") in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_params_and_files_input_and_directory_output(remote_base_path,
#                                                                        file_name_1,
#                                                                        file_name_2):
#     with TemporaryDirectory() as temp_dir:
#         input_file_path_1 = join(testdir, file_name_1)
#         input_file_path_2 = join(testdir, file_name_2)
#         output_path = join(testdir, 'input')  # write output files to input dir
#         output_file_path_1 = join(output_path, f"{file_name_1}.{message}.output")
#         output_file_path_2 = join(output_path, f"{file_name_2}.{message}.output")
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_params_and_files_input_and_directory_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='cat $INPUT | tee $INPUT.$TAG.output',
#             input={
#                 'kind': 'files',
#                 'from': remote_path,
#             },
#             output={
#                 'to': remote_path,
#                 'from': 'input',  # write output files to input dir
#                 'include': {
#                     'patterns': ['output'],
#                     'names': []
#                 }
#             },
#             parameters=[
#                 {
#                     'key': 'TAG',
#                     'value': message
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # prep file
#             with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
#                 file1.write('Hello, 1!')
#                 file2.write('Hello, 2!')
#             store.push_file(input_file_path_1, remote_path)
#             store.push_file(input_file_path_2, remote_path)
#
#             # expect 2 containers
#             Runner(store).run(plan)
#
#             # check files were written locally
#             assert isfile(output_file_path_1)
#             assert isfile(output_file_path_2)
#             check_hello(output_file_path_1, '1')
#             check_hello(output_file_path_2, '2')
#             remove(output_file_path_1)
#             remove(output_file_path_2)
#
#             # check file was pushed to store
#             files = store.list_dir(remote_path)
#             assert join(store.dir, remote_path, f"{file_name_1}.{message}.output") in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_no_params_and_directory_input_and_directory_output(remote_base_path,
#                                                                               file_name_1,
#                                                                               file_name_2):
#     with TemporaryDirectory() as temp_dir:
#         input_file_path_1 = join(testdir, file_name_1)
#         input_file_path_2 = join(testdir, file_name_2)
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_no_params_and_directory_input_and_directory_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='ls $INPUT | tee $INPUT.output',
#             input={
#                 'kind': 'directory',
#                 'from': remote_path,
#             },
#             output={
#                 'to': remote_path,
#                 'from': '',
#                 'include': {
#                     'patterns': ['output'],
#                     'names': []
#                 }
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#         output_path = join(store.dir, f"{join(testdir, 'input')}.output")
#
#         try:
#             # prep file
#             with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
#                 file1.write('Hello, 1!')
#                 file2.write('Hello, 2!')
#             store.push_file(input_file_path_1, remote_path)
#             store.push_file(input_file_path_2, remote_path)
#
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check file was written locally
#             assert isfile(output_path)
#             with open(output_path) as file:
#                 lines = file.readlines()
#                 assert len(lines) == 2
#                 assert input_file_path_1.split('/')[-1] in lines[0]
#                 assert input_file_path_2.split('/')[-1] in lines[1]
#             remove(output_path)
#
#             # check file was pushed to store
#             files = store.list_dir(remote_path)
#             assert join(store.dir, remote_path, output_path.split('/')[-1]) in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_params_and_directory_input_and_directory_output(remote_base_path, file_name_1, file_name_2):
#     with TemporaryDirectory() as temp_dir:
#         input_file_path_1 = join(testdir, file_name_1)
#         input_file_path_2 = join(testdir, file_name_2)
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_params_and_directory_input_and_directory_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='ls $INPUT | tee $INPUT.$TAG.output',
#             input={
#                 'kind': 'directory',
#                 'from': remote_path,
#             },
#             output={
#                 'to': remote_path,
#                 'from': '',
#                 'include': {
#                     'patterns': ['output'],
#                     'names': []
#                 }
#             },
#             parameters=[
#                 {
#                     'key': 'TAG',
#                     'value': message
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#         output_path = join(store.dir, f"{join(testdir, 'input')}.{message}.output")
#
#         try:
#             # prep file
#             with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
#                 file1.write('Hello, 1!')
#                 file2.write('Hello, 2!')
#             store.push_file(input_file_path_1, remote_path)
#             store.push_file(input_file_path_2, remote_path)
#
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check file was written locally
#             assert isfile(output_path)
#             with open(output_path) as file:
#                 lines = file.readlines()
#                 assert len(lines) == 2
#                 assert input_file_path_1.split('/')[-1] in lines[0]
#                 assert input_file_path_2.split('/')[-1] in lines[1]
#             remove(output_path)
#
#             # check file was pushed to store
#             files = store.list_dir(remote_path)
#             assert join(store.dir, remote_path, output_path.split('/')[-1]) in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_fails_with_no_params_and_directory_input_and_directory_output_when_no_inputs_found(remote_base_path):
#     with TemporaryDirectory() as temp_dir:
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_fails_with_no_params_and_directory_input_and_directory_output_when_no_inputs_found',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='ls $INPUT | tee $INPUT.output',
#             input={
#                 'kind': 'directory',
#                 'from': remote_path,
#             },
#             output={
#                 'to': remote_path,
#                 'from': '',
#                 'include': {
#                     'patterns': ['output'],
#                     'names': []
#                 }
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         # expect exception
#         with pytest.raises(ValueError):
#             Runner(store).run(plan)
#
#
# def test_run_fails_with_params_and_directory_input_and_directory_output_when_no_inputs_found(remote_base_path):
#     with TemporaryDirectory() as temp_dir:
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_fails_with_params_and_directory_input_and_directory_output_when_no_inputs_found',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='ls $INPUT | tee $INPUT.$TAG.output',
#             input={
#                 'kind': 'directory',
#                 'from': remote_path,
#             },
#             output={
#                 'to': remote_path,
#                 'from': '',
#                 'include': {
#                     'patterns': ['output'],
#                     'names': []
#                 }
#             },
#             parameters=[
#                 {
#                     'key': 'TAG',
#                     'value': message
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD')
#         )
#         store = LocalStore(temp_dir, plan)
#
#         # expect exception
#         with pytest.raises(ValueError):
#             Runner(store).run(plan)
#
#
# def test_run_succeeds_with_params_and_directory_input_and_filetypes_and_directory_output(remote_base_path, file_name_1, file_name_2):
#     with TemporaryDirectory() as temp_dir:
#         input_file_path_1 = join(testdir, file_name_1)
#         input_file_path_2 = join(testdir, file_name_2)
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_params_and_directory_input_and_directory_output',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='ls $INPUT/*.$FILETYPES | tee $INPUT.$TAG.output',
#             input={
#                 'kind': 'directory',
#                 'from': remote_path,
#                 'filetypes': [
#                     'txt'
#                 ]
#             },
#             output={
#                 'to': remote_path,
#                 'from': '',
#                 'include': {
#                     'patterns': ['output'],
#                 }
#             },
#             parameters=[
#                 {
#                     'key': 'TAG',
#                     'value': message
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#         output_path = join(store.dir, f"{join(testdir, 'input')}.{message}.output")
#
#         try:
#             # prep file
#             with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
#                 file1.write('Hello, 1!')
#                 file2.write('Hello, 2!')
#             store.push_file(input_file_path_1, remote_path)
#             store.push_file(input_file_path_2, remote_path)
#
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check file was written locally
#             assert isfile(output_path)
#             with open(output_path) as file:
#                 lines = file.readlines()
#                 assert len(lines) == 2
#                 assert input_file_path_1.split('/')[-1] in lines[0]
#                 assert input_file_path_2.split('/')[-1] in lines[1]
#             remove(output_path)
#
#             # check file was pushed to store
#             files = store.list_dir(remote_path)
#             assert join(store.dir, remote_path, output_path.split('/')[-1]) in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_include_patterns_and_exclude_names(
#         remote_base_path):
#     with TemporaryDirectory() as temp_dir:
#         output_path = testdir
#         output_file_included = join(output_path, "included.output")
#         output_file_excluded = join(output_path, "excluded.output")
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_excludes',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='touch excluded.output included.output',
#             output={
#                 'to': remote_path,
#                 'from': '',
#                 'include': {
#                     'patterns': ['output'],
#                 },
#                 'exclude': {
#                     'patterns': [],
#                     'names': ['excluded.output']
#                 }
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check files were written locally
#             assert isfile(output_file_included)
#             assert isfile(output_file_excluded)
#             remove(output_file_included)
#             remove(output_file_excluded)
#
#             # check files (including zipped) were pushed to store
#             files = store.list_dir(remote_path)
#             assert len(files) == 2
#             assert join(store.dir, remote_path, 'included.output') in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_params_and_no_input_and_directory_output_with_excludes(remote_base_path):
#     with TemporaryDirectory() as temp_dir:
#         output_path = testdir
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_params_and_no_input_and_directory_output_with_excludes',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='touch excluded.output included.$TAG.output',
#             output={
#                 'to': remote_path,
#                 'include': {
#                     'patterns': ['output'],
#                 },
#                 'exclude': {
#                     'names': ['excluded.output']
#                 }
#             }
#             ,
#             parameters=[
#                 {
#                     'key': 'TAG',
#                     'value': message
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check files were written locally
#             # assert isfile(output_file_included)
#             # assert isfile(output_file_excluded)
#             # remove(output_file_included)
#             # remove(output_file_excluded)
#
#             # check files (including zipped) were pushed to store
#             files = store.list_dir(remote_path)
#             assert len(files) == 2
#             assert join(store.dir, remote_path, f"included.{message}.output") in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes(
#         remote_base_path):
#     with TemporaryDirectory() as temp_dir:
#         output_path = testdir
#         output_file_included = join(output_path, "included.output")
#         output_file_excluded = join(output_path, "excluded.output")
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='touch excluded.output included.output',
#             output={
#                 'to': remote_path,
#                 'from': '',
#                 'include': {
#                     'patterns': ['OUTPUT'],
#                     'names': []
#                 },
#                 'exclude': {
#                     'patterns': [],
#                     'names': ['excluded.output']
#                 }
#             },
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check files were written locally
#             assert isfile(output_file_included)
#             assert isfile(output_file_excluded)
#             remove(output_file_included)
#             remove(output_file_excluded)
#
#             # check files (including zipped) were pushed to store
#             files = store.list_dir(remote_path)
#             assert len(files) == 2
#             assert join(store.dir, remote_path, 'included.output') in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
#
#
# def test_run_succeeds_with_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes(
#         remote_base_path):
#     with TemporaryDirectory() as temp_dir:
#         output_path = testdir
#         output_file_included = join(output_path, f"included.{message}.output")
#         output_file_excluded = join(output_path, "excluded.output")
#         remote_path = join(remote_base_path[1:], "testCollection")
#         plan = RunOptions(
#             identifier='test_run_succeeds_with_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes',
#             workdir=testdir,
#             image="docker://alpine:latest",
#             command='touch excluded.output included.$TAG.output',
#             output={
#                 'to': remote_path,
#                 'from': '',
#                 'include': {
#                     'patterns': ['OUTPUT'],
#                 },
#                 'exclude': {
#                     'names': ['excluded.output']
#                 }
#             },
#             parameters=[
#                 {
#                     'key': 'TAG',
#                     'value': message
#                 },
#             ],
#             docker_username=os.environ.get('DOCKER_USERNAME'),
#             docker_password=os.environ.get('DOCKER_PASSWORD'))
#         store = LocalStore(temp_dir, plan)
#
#         try:
#             # expect 1 container
#             Runner(store).run(plan)
#
#             # check files were written locally
#             assert isfile(output_file_included)
#             assert isfile(output_file_excluded)
#             remove(output_file_included)
#             remove(output_file_excluded)
#
#             # check files (included zipped) were pushed to store
#             files = store.list_dir(remote_path)
#             assert len(files) == 2
#             assert join(store.dir, remote_path, f"included.{message}.output") in files
#             assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
#         finally:
#             clear_dir(testdir)
