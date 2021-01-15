import os
from os import remove, environ
from os.path import join, isfile
from tempfile import TemporaryDirectory

import pytest

from plantit_cli.exceptions import PlantitException
from plantit_cli.runner.runner import Runner
from plantit_cli.config import Config
from plantit_cli.store.local_store import LocalStore
from plantit_cli.tests.test_utils import clear_dir, check_hello

message = "Message"
testdir = environ.get('TEST_DIRECTORY')


# TODO test flow configuration validation

@pytest.mark.skip(reason="y dis fail on CI VM")
def test_run_logs_to_file_when_file_logging_enabled():
    with TemporaryDirectory() as temp_dir:
        log_file_name = 'test_run_logs_to_file_when_file_logging_enabled.txt'
        plan = Config(
            identifier='test_run_succeeds_with_params_and_no_input_and_no_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='echo "$MESSAGE" >> $MESSAGE_FILE',
            params=[
                {
                    'key': 'MESSAGE',
                    'value': message
                },
                {
                    'key': 'MESSAGE_FILE',
                    'value': 'message.txt'
                },
            ],
            logging={
                'file': log_file_name
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)
        try:
            # expect 1 container
            Runner(store).run(plan)

            # check local file was written
            file = join(testdir, 'message.txt')
            assert isfile(file)
            with open(file) as file:
                lines = file.readlines()
                assert len(lines) == 1
                assert lines[0] == f"{message}\n"

            # check log file was written
            log_file = join(testdir, log_file_name)
            assert isfile(log_file)
            with open(log_file) as log_file:
                assert len(log_file.readlines()) > 0
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_no_input_and_no_output():
    with TemporaryDirectory() as temp_dir:
        plan = Config(
            identifier='test_run_succeeds_with_params_and_no_input_and_no_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='echo "$MESSAGE" >> $MESSAGE_FILE',
            params=[
                {
                    'key': 'MESSAGE',
                    'value': message
                },
                {
                    'key': 'MESSAGE_FILE',
                    'value': join(testdir, 'message.txt')
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)
        try:
            # expect 1 container
            Runner(store).run(plan)

            # check local file was written
            file = join(testdir, 'message.txt')
            assert isfile(file)
            with open(file) as file:
                lines = file.readlines()
                assert len(lines) == 1
                assert lines[0] == f"{message}\n"
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_file_input_and_no_output(remote_base_path, file_name_1):
    with TemporaryDirectory() as temp_dir:
        local_path = join(testdir, file_name_1)
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_no_params_and_file_input_and_no_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat "$INPUT" | tee "$INPUT.output"',
            input={
                'kind': 'file',
                'from': join(remote_path, file_name_1),
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # prep file
            with open(local_path, "w") as file1:
                file1.write('Hello, 1!')
            store.upload_file(local_path, remote_path)

            # expect 1 container
            Runner(store).run(plan)

            # check file was pulled
            downloaded_path = join(testdir, 'input', file_name_1)
            check_hello(downloaded_path, 1)
            remove(downloaded_path)

            # check local output file was written
            output_1 = f"{downloaded_path}.output"
            check_hello(output_1, 1)
            remove(output_1)
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_file_input_and_no_output(remote_base_path, file_name_1):
    with TemporaryDirectory() as temp_dir:
        local_path = join(testdir, file_name_1)
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_params_and_file_input_and_no_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat "$INPUT" | tee "$INPUT.$TAG.output"',
            input={
                'kind': 'file',
                'from': join(remote_path, file_name_1),
            },
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # prep file
            with open(local_path, "w") as file1:
                file1.write('Hello, 1!')
            store.upload_file(local_path, remote_path)

            # expect 1 container
            Runner(store).run(plan)

            # check file was pulled
            downloaded_path = join(testdir, 'input', file_name_1)
            check_hello(downloaded_path, 1)
            remove(downloaded_path)

            # check local output file was written
            output_1 = f"{downloaded_path}.{message}.output"
            check_hello(output_1, 1)
            remove(output_1)
        finally:
            clear_dir(testdir)


def test_run_fails_with_no_params_and_file_input_and_no_output_when_no_inputs_found(remote_base_path, file_name_1):
    with TemporaryDirectory() as temp_dir:
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_fails_with_no_params_and_file_input_and_no_output_when_no_inputs_found',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat "$INPUT" | tee "$INPUT.output"',
            input={
                'kind': 'file',
                'from': join(remote_path, file_name_1),
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        # expect exception
        with pytest.raises(FileNotFoundError):
            Runner(store).run(plan)


def test_run_fails_with_params_and_file_input_and_no_output_when_no_inputs_found(remote_base_path, file_name_1):
    with TemporaryDirectory() as temp_dir:
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_fails_with_params_and_file_input_and_no_output_when_no_inputs_found',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat "$INPUT" | tee "$INPUT.$TAG.output"',
            input={
                'kind': 'file',
                'from': join(remote_path, file_name_1),
            },
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # expect exception
            with pytest.raises(FileNotFoundError):
                Runner(store).run(plan)
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_files_input_and_no_output(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        local_path_1 = join(testdir, file_name_1)
        local_path_2 = join(testdir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_no_params_and_files_input_and_no_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat $INPUT | tee $INPUT.output',
            input={
                'kind': 'files',
                'from': remote_path,
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # prep files
            with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            store.upload_file(local_path_1, remote_path)
            store.upload_file(local_path_2, remote_path)

            # expect 2 containers
            Runner(store).run(plan)

            # check files were pulled
            downloaded_path_1 = join(testdir, 'input', file_name_1)
            downloaded_path_2 = join(testdir, 'input', file_name_2)
            check_hello(downloaded_path_1, 1)
            check_hello(downloaded_path_2, 2)
            remove(downloaded_path_1)
            remove(downloaded_path_2)

            # check local output files were written
            output_1 = f"{downloaded_path_1}.output"
            output_2 = f"{downloaded_path_2}.output"
            check_hello(output_1, 1)
            check_hello(output_2, 2)
            remove(output_1)
            remove(output_2)
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_files_input_and_no_output(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        local_path_1 = join(testdir, file_name_1)
        local_path_2 = join(testdir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_params_and_files_input_and_no_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat $INPUT | tee $INPUT.$TAG.output',
            input={
                'kind': 'files',
                'from': remote_path,
            },
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # prep files
            with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            store.upload_file(local_path_1, remote_path)
            store.upload_file(local_path_2, remote_path)

            # expect 2 containers
            Runner(store).run(plan)

            # check files were pulled
            downloaded_path_1 = join(testdir, 'input', file_name_1)
            downloaded_path_2 = join(testdir, 'input', file_name_2)
            check_hello(downloaded_path_1, 1)
            check_hello(downloaded_path_2, 2)
            remove(downloaded_path_1)
            remove(downloaded_path_2)

            # check local output files were written
            output_1 = f"{downloaded_path_1}.{message}.output"
            output_2 = f"{downloaded_path_2}.{message}.output"
            check_hello(output_1, 1)
            check_hello(output_2, 2)
            remove(output_1)
            remove(output_2)
        finally:
            clear_dir(testdir)


def test_run_fails_with_no_params_and_files_input_and_no_output_when_no_inputs_found(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_fails_with_no_params_and_files_input_and_no_output_when_no_inputs_found',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat $INPUT | tee $INPUT.output',
            input={
                'kind': 'files',
                'from': remote_path,
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        # expect exception
        with pytest.raises(PlantitException):
            Runner(store).run(plan)


def test_run_fails_with_params_and_files_input_and_no_output_when_no_inputs_found(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_fails_with_params_and_files_input_and_no_output_when_no_inputs_found',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat $INPUT | tee $INPUT.$TAG.output',
            input={
                'kind': 'files',
                'from': remote_path,
            },
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        # expect exception
        with pytest.raises(PlantitException):
            Runner(store).run(plan)


def test_run_succeeds_with_no_params_and_no_input_and_file_output(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        output_path = join(testdir, 'output.txt')
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_no_params_and_no_input_and_file_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='echo "Hello, world!" >> output.txt',
            output={
                'to': remote_path,
                'include': {
                    'names': ['output.txt']
                }
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # expect 1 container
            Runner(store).run(plan)

            # check file was pushed
            files = store.list_directory(remote_path)
            assert join(store.dir, remote_path, 'output.txt') in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_no_input_and_file_output(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_params_and_no_input_and_file_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='echo "Hello, world!" >> "$TAG".txt',
            output={
                'to': remote_path,
                'include': {
                    'patterns': ["txt"]
                }
            },
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # expect 1 container
            Runner(store).run(plan)

            # check file was pushed
            files = store.list_directory(remote_path)
            assert join(store.dir, remote_path, f"{message}.txt") in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_no_input_and_directory_output(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        output_path = testdir
        output_file_1 = join(output_path, 't1.txt')
        output_file_2 = join(output_path, 't2.txt')
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='echo "Hello, world!" | tee $OUTPUT/t1.txt $OUTPUT/t2.txt',
            output={
                'to': remote_path,
                'from': '',
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # expect 1 container
            Runner(store).run(plan)

            # check files were written locally
            assert isfile(output_file_1)
            assert isfile(output_file_2)
            check_hello(output_file_1, 'world')
            check_hello(output_file_2, 'world')
            remove(output_file_1)
            remove(output_file_2)

            # check files were pushed
            files = store.list_directory(remote_path)
            assert join(store.dir, remote_path, 't1.txt') in files
            assert join(store.dir, remote_path, 't2.txt') in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_no_input_and_directory_output(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        output_path = testdir
        output_file_1 = join(output_path, f"t1.{message}.txt")
        output_file_2 = join(output_path, f"t2.{message}.txt")
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_params_and_no_input_and_directory_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='echo "Hello, world!" | tee $OUTPUT/t1.$TAG.txt $OUTPUT/t2.$TAG.txt',
            output={
                'to': remote_path,
                'from': '',
            },
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # expect 1 container
            Runner(store).run(plan)

            # check files were written locally
            assert isfile(output_file_1)
            assert isfile(output_file_2)
            check_hello(output_file_1, 'world')
            check_hello(output_file_2, 'world')
            remove(output_file_1)
            remove(output_file_2)

            # check files were pushed
            files = store.list_directory(remote_path)
            assert join(store.dir, remote_path, f"t1.{message}.txt") in files
            assert join(store.dir, remote_path, f"t2.{message}.txt") in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_file_input_and_directory_output(remote_base_path, file_name_1):
    with TemporaryDirectory() as temp_dir:
        input_file_path = join(testdir, file_name_1)
        output_path = join(testdir, 'input')  # write output files to input dir
        output_file_path = join(output_path, f"{file_name_1}.output")
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_no_params_and_file_input_and_directory_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat $INPUT | tee $INPUT.output',
            input={
                'kind': 'file',
                'from': join(remote_path, file_name_1),
            },
            output={
                'to': remote_path,
                'from': 'input',  # write output files to input dir
                'include': {
                    'patterns': ['output'],
                    'names': []}
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # prep file
            with open(input_file_path, "w") as file1:
                file1.write('Hello, 1!')
            store.upload_file(input_file_path, remote_path)

            # expect 1 container
            Runner(store).run(plan)

            # check file was written locally
            assert isfile(output_file_path)
            check_hello(output_file_path, '1')
            remove(output_file_path)

            # check file was pushed to store
            files = store.list_directory(remote_path)
            assert join(store.dir, remote_path, f"{file_name_1}.output") in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_file_input_and_directory_output(remote_base_path, file_name_1):
    with TemporaryDirectory() as temp_dir:
        input_file_path = join(testdir, file_name_1)
        output_path = join(testdir, 'input')  # write output files to input dir
        output_file_path = join(output_path, f"{file_name_1}.{message}.output")
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_params_and_file_input_and_directory_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat $INPUT | tee $INPUT.$TAG.output',
            input={
                'kind': 'file',
                'from': join(remote_path, file_name_1),
            },
            output={
                'to': remote_path,
                'from': 'input',  # write output files to input dir
                'include': {
                    'patterns': ['output'],
                    'names': []
                }
            },
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # prep file
            with open(input_file_path, "w") as file1:
                file1.write('Hello, 1!')
            store.upload_file(input_file_path, remote_path)

            # expect 1 container
            Runner(store).run(plan)

            # check file was written locally
            assert isfile(output_file_path)
            check_hello(output_file_path, '1')
            remove(output_file_path)

            # check file was pushed to store
            files = store.list_directory(remote_path)
            assert join(store.dir, remote_path, f"{file_name_1}.{message}.output") in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_files_input_and_directory_output(remote_base_path,
                                                                          file_name_1,
                                                                          file_name_2):
    with TemporaryDirectory() as temp_dir:
        input_file_path_1 = join(testdir, file_name_1)
        input_file_path_2 = join(testdir, file_name_2)
        output_path = join(testdir, 'input')  # write output files to input dir
        output_file_path_1 = join(output_path, f"{file_name_1}.output")
        output_file_path_2 = join(output_path, f"{file_name_2}.output")
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_no_params_and_files_input_and_directory_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat $INPUT | tee $INPUT.output',
            input={
                'kind': 'files',
                'from': remote_path,
            },
            output={
                'to': remote_path,
                'from': 'input',  # write output files to input dir
                'include': {
                    'patterns': ['output'],
                    'names': []
                }
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # prep file
            with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            store.upload_file(input_file_path_1, remote_path)
            store.upload_file(input_file_path_2, remote_path)

            # expect 2 containers
            Runner(store).run(plan)

            # check files were written locally
            assert isfile(output_file_path_1)
            assert isfile(output_file_path_2)
            check_hello(output_file_path_1, '1')
            check_hello(output_file_path_2, '2')
            remove(output_file_path_1)
            remove(output_file_path_2)

            # check file was pushed to store
            files = store.list_directory(remote_path)
            assert join(store.dir, remote_path, f"{file_name_1}.output") in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_files_input_and_directory_output(remote_base_path,
                                                                       file_name_1,
                                                                       file_name_2):
    with TemporaryDirectory() as temp_dir:
        input_file_path_1 = join(testdir, file_name_1)
        input_file_path_2 = join(testdir, file_name_2)
        output_path = join(testdir, 'input')  # write output files to input dir
        output_file_path_1 = join(output_path, f"{file_name_1}.{message}.output")
        output_file_path_2 = join(output_path, f"{file_name_2}.{message}.output")
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_params_and_files_input_and_directory_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat $INPUT | tee $INPUT.$TAG.output',
            input={
                'kind': 'files',
                'from': remote_path,
            },
            output={
                'to': remote_path,
                'from': 'input',  # write output files to input dir
                'include': {
                    'patterns': ['output'],
                    'names': []
                }
            },
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # prep file
            with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            store.upload_file(input_file_path_1, remote_path)
            store.upload_file(input_file_path_2, remote_path)

            # expect 2 containers
            Runner(store).run(plan)

            # check files were written locally
            assert isfile(output_file_path_1)
            assert isfile(output_file_path_2)
            check_hello(output_file_path_1, '1')
            check_hello(output_file_path_2, '2')
            remove(output_file_path_1)
            remove(output_file_path_2)

            # check file was pushed to store
            files = store.list_directory(remote_path)
            assert join(store.dir, remote_path, f"{file_name_1}.{message}.output") in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_directory_input_and_directory_output(remote_base_path,
                                                                              file_name_1,
                                                                              file_name_2):
    with TemporaryDirectory() as temp_dir:
        input_file_path_1 = join(testdir, file_name_1)
        input_file_path_2 = join(testdir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_no_params_and_directory_input_and_directory_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='ls $INPUT | tee $INPUT.output',
            input={
                'kind': 'directory',
                'from': remote_path,
            },
            output={
                'to': remote_path,
                'from': '',
                'include': {
                    'patterns': ['output'],
                    'names': []
                }
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)
        output_path = join(store.dir, f"{join(testdir, 'input')}.output")

        try:
            # prep file
            with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            store.upload_file(input_file_path_1, remote_path)
            store.upload_file(input_file_path_2, remote_path)

            # expect 1 container
            Runner(store).run(plan)

            # check file was written locally
            assert isfile(output_path)
            with open(output_path) as file:
                lines = file.readlines()
                assert len(lines) == 2
                assert input_file_path_1.split('/')[-1] in lines[0]
                assert input_file_path_2.split('/')[-1] in lines[1]
            remove(output_path)

            # check file was pushed to store
            files = store.list_directory(remote_path)
            assert join(store.dir, remote_path, output_path.split('/')[-1]) in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_directory_input_and_directory_output(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        input_file_path_1 = join(testdir, file_name_1)
        input_file_path_2 = join(testdir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_params_and_directory_input_and_directory_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='ls $INPUT | tee $INPUT.$TAG.output',
            input={
                'kind': 'directory',
                'from': remote_path,
            },
            output={
                'to': remote_path,
                'from': '',
                'include': {
                    'patterns': ['output'],
                    'names': []
                }
            },
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)
        output_path = join(store.dir, f"{join(testdir, 'input')}.{message}.output")

        try:
            # prep file
            with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            store.upload_file(input_file_path_1, remote_path)
            store.upload_file(input_file_path_2, remote_path)

            # expect 1 container
            Runner(store).run(plan)

            # check file was written locally
            assert isfile(output_path)
            with open(output_path) as file:
                lines = file.readlines()
                assert len(lines) == 2
                assert input_file_path_1.split('/')[-1] in lines[0]
                assert input_file_path_2.split('/')[-1] in lines[1]
            remove(output_path)

            # check file was pushed to store
            files = store.list_directory(remote_path)
            assert join(store.dir, remote_path, output_path.split('/')[-1]) in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_fails_with_no_params_and_directory_input_and_directory_output_when_no_inputs_found(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_fails_with_no_params_and_directory_input_and_directory_output_when_no_inputs_found',
            workdir=testdir,
            image="docker://alpine:latest",
            command='ls $INPUT | tee $INPUT.output',
            input={
                'kind': 'directory',
                'from': remote_path,
            },
            output={
                'to': remote_path,
                'from': '',
                'include': {
                    'patterns': ['output'],
                    'names': []
                }
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        # expect exception
        with pytest.raises(PlantitException):
            Runner(store).run(plan)


def test_run_fails_with_params_and_directory_input_and_directory_output_when_no_inputs_found(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_fails_with_params_and_directory_input_and_directory_output_when_no_inputs_found',
            workdir=testdir,
            image="docker://alpine:latest",
            command='ls $INPUT | tee $INPUT.$TAG.output',
            input={
                'kind': 'directory',
                'from': remote_path,
            },
            output={
                'to': remote_path,
                'from': '',
                'include': {
                    'patterns': ['output'],
                    'names': []
                }
            },
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD')
        )
        store = LocalStore(temp_dir, plan)

        # expect exception
        with pytest.raises(PlantitException):
            Runner(store).run(plan)


def test_run_succeeds_with_params_and_directory_input_and_filetypes_and_directory_output(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        input_file_path_1 = join(testdir, file_name_1)
        input_file_path_2 = join(testdir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_params_and_directory_input_and_directory_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='ls $INPUT/*.$FILETYPES | tee $INPUT.$TAG.output',
            input={
                'kind': 'directory',
                'from': remote_path,
                'filetypes': [
                    'txt'
                ]
            },
            output={
                'to': remote_path,
                'from': '',
                'include': {
                    'patterns': ['output'],
                }
            },
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)
        output_path = join(store.dir, f"{join(testdir, 'input')}.{message}.output")

        try:
            # prep file
            with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            store.upload_file(input_file_path_1, remote_path)
            store.upload_file(input_file_path_2, remote_path)

            # expect 1 container
            Runner(store).run(plan)

            # check file was written locally
            assert isfile(output_path)
            with open(output_path) as file:
                lines = file.readlines()
                assert len(lines) == 2
                assert input_file_path_1.split('/')[-1] in lines[0]
                assert input_file_path_2.split('/')[-1] in lines[1]
            remove(output_path)

            # check file was pushed to store
            files = store.list_directory(remote_path)
            assert join(store.dir, remote_path, output_path.split('/')[-1]) in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_include_patterns_and_exclude_names(
        remote_base_path):
    with TemporaryDirectory() as temp_dir:
        output_path = testdir
        output_file_included = join(output_path, "included.output")
        output_file_excluded = join(output_path, "excluded.output")
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_excludes',
            workdir=testdir,
            image="docker://alpine:latest",
            command='touch excluded.output included.output',
            output={
                'to': remote_path,
                'from': '',
                'include': {
                    'patterns': ['output'],
                },
                'exclude': {
                    'patterns': [],
                    'names': ['excluded.output']
                }
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # expect 1 container
            Runner(store).run(plan)

            # check files were written locally
            assert isfile(output_file_included)
            assert isfile(output_file_excluded)
            remove(output_file_included)
            remove(output_file_excluded)

            # check files (including zipped) were pushed to store
            files = store.list_directory(remote_path)
            assert len(files) == 2
            assert join(store.dir, remote_path, 'included.output') in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_no_input_and_directory_output_with_excludes(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        output_path = testdir
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_params_and_no_input_and_directory_output_with_excludes',
            workdir=testdir,
            image="docker://alpine:latest",
            command='touch excluded.output included.$TAG.output',
            output={
                'to': remote_path,
                'include': {
                    'patterns': ['output'],
                },
                'exclude': {
                    'names': ['excluded.output']
                }
            }
            ,
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # expect 1 container
            Runner(store).run(plan)

            # check files were written locally
            # assert isfile(output_file_included)
            # assert isfile(output_file_excluded)
            # remove(output_file_included)
            # remove(output_file_excluded)

            # check files (including zipped) were pushed to store
            files = store.list_directory(remote_path)
            assert len(files) == 2
            assert join(store.dir, remote_path, f"included.{message}.output") in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes(
        remote_base_path):
    with TemporaryDirectory() as temp_dir:
        output_path = testdir
        output_file_included = join(output_path, "included.output")
        output_file_excluded = join(output_path, "excluded.output")
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes',
            workdir=testdir,
            image="docker://alpine:latest",
            command='touch excluded.output included.output',
            output={
                'to': remote_path,
                'from': '',
                'include': {
                    'patterns': ['OUTPUT'],
                    'names': []
                },
                'exclude': {
                    'patterns': [],
                    'names': ['excluded.output']
                }
            },
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # expect 1 container
            Runner(store).run(plan)

            # check files were written locally
            assert isfile(output_file_included)
            assert isfile(output_file_excluded)
            remove(output_file_included)
            remove(output_file_excluded)

            # check files (including zipped) were pushed to store
            files = store.list_directory(remote_path)
            assert len(files) == 2
            assert join(store.dir, remote_path, 'included.output') in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes(
        remote_base_path):
    with TemporaryDirectory() as temp_dir:
        output_path = testdir
        output_file_included = join(output_path, f"included.{message}.output")
        output_file_excluded = join(output_path, "excluded.output")
        remote_path = join(remote_base_path[1:], "testCollection")
        plan = Config(
            identifier='test_run_succeeds_with_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes',
            workdir=testdir,
            image="docker://alpine:latest",
            command='touch excluded.output included.$TAG.output',
            output={
                'to': remote_path,
                'from': '',
                'include': {
                    'patterns': ['OUTPUT'],
                },
                'exclude': {
                    'names': ['excluded.output']
                }
            },
            params=[
                {
                    'key': 'TAG',
                    'value': message
                },
            ],
            docker_username=os.environ.get('DOCKER_USERNAME'),
            docker_password=os.environ.get('DOCKER_PASSWORD'))
        store = LocalStore(temp_dir, plan)

        try:
            # expect 1 container
            Runner(store).run(plan)

            # check files were written locally
            assert isfile(output_file_included)
            assert isfile(output_file_excluded)
            remove(output_file_included)
            remove(output_file_excluded)

            # check files (included zipped) were pushed to store
            files = store.list_directory(remote_path)
            assert len(files) == 2
            assert join(store.dir, remote_path, f"included.{message}.output") in files
            assert join(store.dir, remote_path, f"{plan.identifier}.zip") in files
        finally:
            clear_dir(testdir)
