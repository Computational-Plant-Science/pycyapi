from os import environ, remove
from os.path import join, isfile
from tempfile import TemporaryDirectory

import pytest
from tempfile import TemporaryDirectory

import plantit_cli.runner.dask_commands
from plantit_cli import commands
from plantit_cli.runner import dask_commands
from plantit_cli.tests.utils import clear_dir, check_hello, TerrainToken

message = "Message"
token = TerrainToken.get()


def test_run_parameters():
    with TemporaryDirectory() as test_dir:
        try:
            output_file_path = join(test_dir, 'output.txt')
            options = {
                'workdir': test_dir,
                'image': 'docker://alpine',
                'command': 'echo "$MESSAGE" > $WORKDIR/output.txt',
                'parameters': [{'key': 'MESSAGE', 'value': message}],
            }
            dask_commands.run_dask(
                options=options,
                docker_username=environ.get('DOCKER_USERNAME', None),
                docker_password=environ.get('DOCKER_PASSWORD', None),
                docker=False)

            assert isfile(output_file_path)
            with open(output_file_path) as output_file:
                lines = output_file.readlines()
                assert len(lines) >= 1
                assert any(message in line for line in lines)
        except:
            clear_dir(test_dir)
            raise
        finally:
            clear_dir(test_dir)


def test_run_bind_mounts():
    with TemporaryDirectory() as test_dir:
        try:
            output_file_path = join(test_dir, 'output.txt')
            options = {
                'workdir': test_dir,
                'image': 'docker://alpine',
                'command': f"ls > $WORKDIR/output.txt",
                'bind_mounts': [{'host_path': '/opt/plantit-cli/samples', 'container_path': test_dir}],
            }
            dask_commands.run_dask(
                options=options,
                docker_username=environ.get('DOCKER_USERNAME', None),
                docker_password=environ.get('DOCKER_PASSWORD', None))

            assert isfile(output_file_path)
            with open(output_file_path) as file:
                lines = file.readlines()
                assert len(lines) >= 1
                print(lines)
                assert any('flow_with_bind_mounts' in line for line in lines)
        except:
            clear_dir(test_dir)
            raise
        finally:
            clear_dir(test_dir)


def test_run_directory_input(file_name_1, file_name_2):
    with TemporaryDirectory() as test_dir:
        try:
            with TemporaryDirectory() as temp_dir:
                input_file_path_1 = join(temp_dir, file_name_1)
                input_file_path_2 = join(temp_dir, file_name_2)
                output_file_path = join(test_dir, 'output.txt')
                with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
                    file1.write('Hello, 1!')
                    file2.write('Hello, 2!')

                options = {
                    'workdir': test_dir,
                    'image': 'docker://alpine',
                    'command': 'pwd > $WORKDIR/output.txt',
                    'input': {'path': temp_dir, 'kind': 'directory'},
                }
                dask_commands.run_dask(
                    options=options,
                    docker_username=environ.get('DOCKER_USERNAME', None),
                    docker_password=environ.get('DOCKER_PASSWORD', None))

                assert isfile(output_file_path)
                with open(output_file_path) as output_file:
                    lines = output_file.readlines()
                    assert len(lines) == 1
                    assert test_dir in lines[0]
        except:
            clear_dir(test_dir)
            raise
        finally:
            clear_dir(test_dir)


def test_run_files_input(file_name_1, file_name_2):
    with TemporaryDirectory() as test_dir:
        try:
            input_file_path_1 = join(test_dir, file_name_1)
            input_file_path_2 = join(test_dir, file_name_2)
            output_file_path = join(test_dir, 'output.txt')
            with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            options = {
                'workdir': test_dir,
                'image': 'docker://alpine',
                'command': 'echo $INPUT >> $WORKDIR/output.txt',
                'input': {'path': test_dir, 'kind': 'files'},
            }
            dask_commands.run_dask(
                options=options,
                docker_username=environ.get('DOCKER_USERNAME', None),
                docker_password=environ.get('DOCKER_PASSWORD', None))

            assert isfile(output_file_path)
            with open(output_file_path) as output_file:
                lines = output_file.readlines()
                assert len(lines) >= 2
                assert any(file_name_1 in line for line in lines)
                assert any(file_name_2 in line for line in lines)
        except:
            clear_dir(test_dir)
            raise
        finally:
            clear_dir(test_dir)


def test_run_file_input(file_name_1):
    with TemporaryDirectory() as test_dir:
        try:
            input_file_path = join(test_dir, file_name_1)
            output_file_path = join(test_dir, 'output.txt')
            with open(input_file_path, "w") as file1:
                file1.write(message)

            options = {
                'workdir': test_dir,
                'image': 'docker://alpine',
                'command': 'cat $INPUT > $WORKDIR/output.txt',
                'input': {'path': input_file_path, 'kind': 'file'},
            }
            dask_commands.run_dask(
                options=options,
                docker_username=environ.get('DOCKER_USERNAME', None),
                docker_password=environ.get('DOCKER_PASSWORD', None))

            assert isfile(output_file_path)
            with open(output_file_path) as output_file:
                lines = output_file.readlines()
                assert len(lines) >= 1
                assert any(message in line for line in lines)
        except:
            clear_dir(test_dir)
            raise
        finally:
            clear_dir(test_dir)
