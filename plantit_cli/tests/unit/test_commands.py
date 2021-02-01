import zipfile
from os import environ, listdir
from os.path import join, isfile
from tempfile import TemporaryDirectory

import pytest

from plantit_cli import commands
from plantit_cli.options import RunOptions, FileInput, FilesInput, DirectoryInput, Parameter, BindMount
from plantit_cli.tests.utils import clear_dir

message = "Message"
test_dir = environ.get('TEST_DIRECTORY')


def test_zip_all_files_are_included_by_default(file_name_1, file_name_2):
    zip_stem = 'test_zip'
    zip_name = f"{zip_stem}.zip"

    try:
        with TemporaryDirectory() as temp_dir:
            with open(join(temp_dir, file_name_1), "w") as file1, \
                    open(join(temp_dir, file_name_2), "w") as file2, \
                    open(join(temp_dir, 'excluded'), "w") as file3:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
                file3.write('Hello, 3!')

            commands.zip(input_dir=temp_dir, output_dir=test_dir, name=zip_stem)

        assert isfile(join(test_dir, zip_name))

        with zipfile.ZipFile(join(test_dir, zip_name), 'r') as zip_file:
            zip_file.extractall(test_dir)
            print(listdir(test_dir))
            assert isfile(join(test_dir, file_name_1))
            assert isfile(join(test_dir, file_name_2))
    finally:
        clear_dir(test_dir)


def test_zip_throws_error_when_total_size_exceeds_max(file_name_1, file_name_2):
    zip_stem = 'test_zip'
    zip_name = f"{zip_stem}.zip"

    try:
        with TemporaryDirectory() as temp_dir:
            with open(join(temp_dir, file_name_1), "w") as file1, \
                    open(join(temp_dir, file_name_2), "w") as file2, \
                    open(join(temp_dir, 'excluded'), "w") as file3:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
                file3.write('Hello, 3!')

            with pytest.raises(ValueError):
                commands.zip(input_dir=temp_dir, output_dir=test_dir, name=zip_stem, max_size=10)

        assert not isfile(join(test_dir, zip_name))
    finally:
        clear_dir(test_dir)


def test_run_parameters(file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        input_file_path_1 = join(temp_dir, file_name_1)
        input_file_path_2 = join(temp_dir, file_name_2)
        output_file_path = join(temp_dir, 'output.txt')
        with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')

        options = RunOptions(
            workdir=temp_dir,
            image='docker://alpine',
            command='echo "$MESSAGE" > $WORKDIR/output.txt',
            parameters=[Parameter(key='MESSAGE', value=message)])
        commands.run(
            options=options,
            docker_username=environ.get('DOCKER_USERNAME', None),
            docker_password=environ.get('DOCKER_PASSWORD', None))

        assert isfile(output_file_path)
        with open(output_file_path) as output_file:
            lines = output_file.readlines()
            assert len(lines) >= 1
            assert any(message in line for line in lines)


def test_run_bind_mounts(file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        input_file_path_1 = join(test_dir, file_name_1)
        input_file_path_2 = join(test_dir, file_name_2)
        output_file_path = join(test_dir, 'output.txt')
        with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')

        options = RunOptions(
            workdir=test_dir,
            image='docker://alpine',
            command=f"echo '{message}' > $WORKDIR/output.txt",
            bind_mounts=[BindMount(host_path=temp_dir, container_path=test_dir)])
        commands.run(
            options=options,
            docker_username=environ.get('DOCKER_USERNAME', None),
            docker_password=environ.get('DOCKER_PASSWORD', None))

        assert isfile(output_file_path)
        with open(output_file_path) as output_file:
            lines = output_file.readlines()
            assert len(lines) >= 1
            assert any(message in line for line in lines)


def test_run_directory_input(file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        input_file_path_1 = join(temp_dir, file_name_1)
        input_file_path_2 = join(temp_dir, file_name_2)
        output_file_path = join(temp_dir, 'output.txt')
        with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')

        options = RunOptions(
            workdir=temp_dir,
            image='docker://alpine',
            command='ls "$INPUT" > $WORKDIR/output.txt',
            input=DirectoryInput(path=temp_dir))
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


def test_run_files_input(file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        input_file_path_1 = join(temp_dir, file_name_1)
        input_file_path_2 = join(temp_dir, file_name_2)
        output_file_path = join(temp_dir, 'output.txt')
        with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')

        options = RunOptions(
            workdir=temp_dir,
            image='docker://alpine',
            command='echo "$INPUT" >> $WORKDIR/output.txt',
            input=FilesInput(path=temp_dir))
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


def test_run_file_input(file_name_1):
    with TemporaryDirectory() as temp_dir:
        input_file_path = join(temp_dir, file_name_1)
        output_file_path = join(temp_dir, 'output.txt')
        with open(input_file_path, "w") as file1:
            file1.write(message)

        options = RunOptions(
            workdir=temp_dir,
            image='docker://alpine',
            command='cat "$INPUT" > $WORKDIR/output.txt',
            input=FileInput(path=input_file_path))
        commands.run(
            options=options,
            docker_username=environ.get('DOCKER_USERNAME', None),
            docker_password=environ.get('DOCKER_PASSWORD', None))

        assert isfile(output_file_path)
        with open(output_file_path) as output_file:
            lines = output_file.readlines()
            assert len(lines) >= 1
            assert any(message in line for line in lines)
