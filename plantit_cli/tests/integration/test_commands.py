from os import environ, remove
from os.path import join, isfile
from tempfile import TemporaryDirectory

import pytest
from tempfile import TemporaryDirectory

from plantit_cli import commands
from plantit_cli.store import terrain_commands
from plantit_cli.tests.utils import clear_dir, check_hello, TerrainToken

message = "Message"
token = TerrainToken.get()
jobqueue = {
    'slurm': {
        'cores': 1,
        'processes': 1,
        'memory': '1GB',
        'walltime': '00:01:00'
    }
}


@pytest.mark.skip(reason='debug')
def test_terrain_pull(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir, TemporaryDirectory() as test_dir:
        local_path_1 = join(test_dir, file_name_1)
        local_path_2 = join(test_dir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # prep files
            with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # TODO push files to remote directory

            # pull directory
            terrain_commands.pull(remote_path, test_dir, token)

            # check files were pulled
            downloaded_path_1 = join(test_dir, file_name_1)
            downloaded_path_2 = join(test_dir, file_name_2)
            check_hello(downloaded_path_1, 1)
            check_hello(downloaded_path_2, 2)
            remove(downloaded_path_1)
            remove(downloaded_path_2)
        finally:
            clear_dir(test_dir)


@pytest.mark.skip(reason='debug')
def test_terrain_push(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir, TemporaryDirectory() as test_dir:
        local_path_1 = join(test_dir, file_name_1)
        local_path_2 = join(test_dir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # prep files
            with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # TODO push files to remote directory

            # push directory
            terrain_commands.push(test_dir, remote_path, token)

            # check files were pushed
            uploaded_path_1 = join(remote_path, file_name_1)
            uploaded_path_2 = join(remote_path, file_name_2)

            # TODO check files (included zipped) were pushed to store
            # files = store.list_dir(remote_path)
            # print(files)
            # assert len(files) == 2
            # assert store.file_exists(uploaded_path_1)
            # assert store.file_exists(uploaded_path_2)
        finally:
            clear_dir(test_dir)


@pytest.mark.skip(reason="debug")
def test_run_parameters_local():
    with TemporaryDirectory() as test_dir:
        try:
            output_file_path = join(test_dir, 'output.txt')
            options = {
                'workdir': test_dir,
                'image': 'docker://alpine',
                'command': 'echo "$MESSAGE" > $WORKDIR/output.txt',
                'parameters': [{'key': 'MESSAGE', 'value': message}],
            }
            commands.run(
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


@pytest.mark.skip(reason="debug")
def test_run_parameters_slurm():
    with TemporaryDirectory() as test_dir:
        try:
            output_file_path = join(test_dir, 'output.txt')
            options = {
                'workdir': test_dir,
                'image': 'docker://alpine',
                'command': 'echo "$MESSAGE" > $WORKDIR/output.txt',
                'parameters': [{'key': 'MESSAGE', 'value': message}],
                'jobqueue': jobqueue
            }
            commands.run(
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


@pytest.mark.skip(reason="debug")
def test_run_bind_mounts_local():
    with TemporaryDirectory() as test_dir:
        try:
            output_file_path = join(test_dir, 'output.txt')
            options = {
                'workdir': test_dir,
                'image': 'docker://alpine',
                'command': f"ls > $WORKDIR/output.txt",
                'bind_mounts': [{'host_path': '/opt/plantit-cli/samples', 'container_path': test_dir}],
            }
            commands.run(
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


@pytest.mark.skip(reason="debug")
def test_run_bind_mounts_slurm():
    with TemporaryDirectory() as test_dir:
        try:
            output_file_path = join(test_dir, 'output.txt')
            options = {
                'workdir': test_dir,
                'image': 'docker://alpine',
                'command': f"ls > $WORKDIR/output.txt",
                'bind_mounts': [{'host_path': '/opt/plantit-cli/samples', 'container_path': test_dir}],
                'jobqueue': jobqueue
            }
            commands.run(
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


@pytest.mark.skip(reason="debug")
def test_run_directory_input_local(file_name_1, file_name_2):
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
                commands.run(
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


@pytest.mark.skip(reason="debug")
def test_run_directory_input_slurm(file_name_1, file_name_2):
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
                    'jobqueue': jobqueue
                }
                commands.run(
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


@pytest.mark.skip(reason="debug")
def test_run_files_input_local(file_name_1, file_name_2):
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
            commands.run(
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


@pytest.mark.skip(reason="debug")
def test_run_files_input_slurm(file_name_1, file_name_2):
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
                'jobqueue': jobqueue
            }
            commands.run(
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


@pytest.mark.skip(reason="debug")
def test_run_file_input_local(file_name_1):
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
            commands.run(
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


@pytest.mark.skip(reason="debug")
def test_run_file_input_slurm(file_name_1):
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
                'jobqueue': jobqueue
            }
            commands.run(
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
