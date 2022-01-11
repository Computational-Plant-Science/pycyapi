from os import environ
from os.path import join, isfile
from tempfile import TemporaryDirectory

from plantit_cli.runner import dask_commands
from plantit_cli.tests.utils import clear_dir, TerrainToken

message = "Message!"
token = TerrainToken.get()


def test_run_parameters_docker():
    with TemporaryDirectory() as testdir:
        options = {
            'workdir': testdir,
            'image': 'docker://alpine',
            'command': 'echo "$MESSAGE" > $WORKDIR/output.txt',
            'parameters': [{'key': 'MESSAGE', 'value': message}]}
        dask_commands.run_dask(
            options=options,
            docker=True,
            docker_username=environ.get('DOCKER_USERNAME', None),
            docker_password=environ.get('DOCKER_PASSWORD', None))

        output_file_path = join(testdir, 'output.txt')
        assert isfile(output_file_path)
        with open(output_file_path) as output_file:
            lines = output_file.readlines()
            assert len(lines) >= 1
            assert any(message in line for line in lines)


def test_run_env_vars_docker():
    with TemporaryDirectory() as testdir:
        message2 = 'another message!'
        options = {
            'workdir': testdir,
            'env': [{'key': 'MESSAGE2', 'value': message2}],
            'image': 'docker://alpine',
            'command': 'echo $MESSAGE2 > $WORKDIR/output.txt',
            #'parameters': [{'key': 'MESSAGE', 'value': message}]
        }
        dask_commands.run_dask(
            options=options,
            docker=True,
            docker_username=environ.get('DOCKER_USERNAME', None),
            docker_password=environ.get('DOCKER_PASSWORD', None))

        output_file_path = join(testdir, 'output.txt')
        assert isfile(output_file_path)
        with open(output_file_path) as output_file:
            lines = output_file.readlines()
            print(lines)
            assert len(lines) >= 1
            assert any(message2 in line for line in lines)


def test_run_bind_mounts_docker(file_name_1, file_name_2):
    with TemporaryDirectory() as tempdir, TemporaryDirectory() as workdir, TemporaryDirectory() as testdir:
        input_file_path_1 = join(testdir, file_name_1)
        input_file_path_2 = join(testdir, file_name_2)
        output_file_path = join(tempdir, 'output.txt')
        with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')

        options = {
            'workdir': workdir,
            'image': 'docker://alpine',
            'command': f"echo '{message}' > {testdir}/output.txt",
            'bind_mounts': [{'host_path': tempdir, 'container_path': testdir}]}
        dask_commands.run_dask(
            options=options,
            docker=True,
            docker_username=environ.get('DOCKER_USERNAME', None),
            docker_password=environ.get('DOCKER_PASSWORD', None))

        assert isfile(output_file_path)
        with open(output_file_path) as output_file:
            lines = output_file.readlines()
            assert len(lines) >= 1
            assert any(message in line for line in lines)


def test_run_directory_input_docker(file_name_1, file_name_2):
    with TemporaryDirectory() as testdir:
        input_file_path_1 = join(testdir, file_name_1)
        input_file_path_2 = join(testdir, file_name_2)
        output_file_path = join(testdir, 'output.txt')
        with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')

        options = {
            'workdir': testdir,
            'image': 'docker://alpine',
            'command': 'ls "$INPUT" > $WORKDIR/output.txt',
            'input': {'path': testdir, 'kind': 'directory'}}
        dask_commands.run_dask(
            options=options,
            docker=True,
            docker_username=environ.get('DOCKER_USERNAME', None),
            docker_password=environ.get('DOCKER_PASSWORD', None))

        assert isfile(output_file_path)
        with open(output_file_path) as output_file:
            lines = output_file.readlines()
            assert len(lines) >= 2
            assert any(file_name_1 in line for line in lines)
            assert any(file_name_2 in line for line in lines)


def test_run_files_input_docker(file_name_1, file_name_2):
    with TemporaryDirectory() as testdir:
        input_file_path_1 = join(testdir, file_name_1)
        input_file_path_2 = join(testdir, file_name_2)
        output_file_path = join(testdir, 'output.txt')
        with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')

        options = {
            'workdir': testdir,
            'image': 'docker://alpine',
            'command': 'echo "$INPUT" >> $WORKDIR/output.txt',
            'input': {'path': testdir, 'kind': 'files'}}
        dask_commands.run_dask(
            options=options,
            docker=True,
            docker_username=environ.get('DOCKER_USERNAME', None),
            docker_password=environ.get('DOCKER_PASSWORD', None))

        assert isfile(output_file_path)
        with open(output_file_path) as output_file:
            lines = output_file.readlines()
            assert len(lines) >= 2
            assert any(file_name_1 in line for line in lines)
            assert any(file_name_2 in line for line in lines)


def test_run_file_input_docker(file_name_1):
    with TemporaryDirectory() as testdir:
        input_file_path = join(testdir, file_name_1)
        output_file_path = join(testdir, 'output.txt')
        with open(input_file_path, "w") as file1:
            file1.write(message)

        options = {
            'workdir': testdir,
            'image': 'docker://alpine',
            'command': 'cat "$INPUT" > $WORKDIR/output.txt',
            'input': {'path': input_file_path, 'kind': 'file'}
        }
        dask_commands.run_dask(
            options=options,
            docker=True,
            docker_username=environ.get('DOCKER_USERNAME', None),
            docker_password=environ.get('DOCKER_PASSWORD', None))

        assert isfile(output_file_path)
        with open(output_file_path) as output_file:
            lines = output_file.readlines()
            assert len(lines) >= 1
            assert any(message in line for line in lines)
