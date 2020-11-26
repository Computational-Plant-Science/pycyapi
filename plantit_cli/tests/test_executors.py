import os
import tempfile
from os.path import join, isfile

from plantit_cli.tests.utils import clear_dir, check_hello, create_collection, upload_file, delete_collection, \
    list_files

message = "Message!"
testdir = '/opt/plantit-cli/runs/'
tempdir = tempfile.gettempdir()


def test_run_with_params(executor, run_with_params):
    try:
        # run the run
        executor.execute(run_with_params)

        # check local message file
        file = join(testdir, 'message.txt')
        assert isfile(file)
        with open(file) as file:
            lines = file.readlines()
            assert len(lines) == 1
            assert lines[0] == f"{message}\n"
    finally:
        clear_dir(testdir)


def test_run_with_single_file_input(
        executor,
        remote_base_path,
        token,
        run_with_single_file_input,
        file_name_1):
    local_path = join(testdir, file_name_1)
    remote_path = join(remote_base_path, "testCollection")

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_path, "w") as file1:
            file1.write('Hello, 1!')
        upload_file(local_path, remote_path, token)

        # expect 1 container
        executor.execute(run_with_single_file_input)

        # check file was pulled
        downloaded_path = join(testdir, 'input', file_name_1)
        check_hello(downloaded_path, 1)
        os.remove(downloaded_path)

        # check local output file was written
        output_1 = f"{downloaded_path}.output"
        check_hello(output_1, 1)
        os.remove(output_1)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_run_with_directory_input_many_files(
        executor,
        remote_base_path,
        token,
        run_with_directory_input_many_files,
        file_name_1,
        file_name_2):
    local_path_1 = join(testdir, file_name_1)
    local_path_2 = join(testdir, file_name_2)
    remote_path = join(remote_base_path, "testCollection")

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep files
        with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')
        upload_file(local_path_1, remote_path, token)
        upload_file(local_path_2, remote_path, token)

        # expect 2 containers
        executor.execute(run_with_directory_input_many_files)

        # check files were pulled
        downloaded_path_1 = join(testdir, 'input', file_name_1)
        downloaded_path_2 = join(testdir, 'input', file_name_2)
        check_hello(downloaded_path_1, 1)
        check_hello(downloaded_path_2, 2)
        os.remove(downloaded_path_1)
        os.remove(downloaded_path_2)

        # check local output files were written
        output_1 = f"{downloaded_path_1}.output"
        output_2 = f"{downloaded_path_2}.output"
        check_hello(output_1, 1)
        check_hello(output_2, 2)
        os.remove(output_1)
        os.remove(output_2)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_run_with_file_output(
        executor,
        remote_base_path,
        token,
        run_with_file_output):
    local_output_path = join(testdir, run_with_file_output.output['from'])
    remote_path = join(remote_base_path, "testCollection")

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # run
        executor.execute(run_with_file_output)

        # check files were written locally
        assert isfile(local_output_path)
        check_hello(local_output_path, 'world')
        # os.remove(local_output_file)

        # check file was pushed to CyVerse
        files = list_files(remote_path, token)
        assert join(remote_path, 'output.txt') in [file['path'] for file in files]
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_run_with_directory_output(
        executor,
        remote_base_path,
        token,
        run_with_directory_output):
    local_output_path = join(testdir, run_with_directory_output.output['from'])
    local_output_file_1 = join(local_output_path, 't1.txt')
    local_output_file_2 = join(local_output_path, 't2.txt')
    remote_path = join(remote_base_path, "testCollection")

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # run
        executor.execute(run_with_directory_output)

        # check files were written locally
        assert isfile(local_output_file_1)
        assert isfile(local_output_file_2)
        check_hello(local_output_file_1, 'world')
        check_hello(local_output_file_2, 'world')
        os.remove(local_output_file_1)
        os.remove(local_output_file_2)

        # check files were pushed to CyVerse
        files = list_files(remote_path, token)
        assert join(remote_path, 't1.txt') in [file['path'] for file in files]
        assert join(remote_path, 't2.txt') in [file['path'] for file in files]
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_run_with_file_input_and_directory_output(
        executor,
        remote_base_path,
        token,
        run_with_file_input_and_directory_output,
        file_name_1):
    local_input_file_path = join(testdir, file_name_1)
    local_output_path = join(testdir, run_with_file_input_and_directory_output.output['from'])
    local_output_file_path = join(local_output_path, f"{file_name_1}.output")
    remote_path = join(remote_base_path, "testCollection")

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_input_file_path, "w") as file1:
            file1.write('Hello, 1!')
        upload_file(local_input_file_path, remote_path, token)

        # expect 1 container
        executor.execute(run_with_file_input_and_directory_output)

        # check file was written locally
        assert isfile(local_output_file_path)
        check_hello(local_output_file_path, '1')
        os.remove(local_output_file_path)

        # check file was pushed to CyVerse
        files = list_files(remote_path, token)
        assert join(remote_path, f"{file_name_1}.output") in [file['path'] for file in files]
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_run_with_directory_output_with_excludes(
        executor,
        remote_base_path,
        token,
        run_with_directory_output_with_excludes):
    local_output_path = join(testdir, run_with_directory_output_with_excludes.output['from'])
    local_output_file_included = join(local_output_path, "included.output")
    local_output_file_excluded = join(local_output_path, "excluded.output")
    remote_path = join(remote_base_path, "testCollection")

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # expect 1 container
        executor.execute(run_with_directory_output_with_excludes)

        # check files were written locally
        assert isfile(local_output_file_included)
        assert isfile(local_output_file_excluded)
        os.remove(local_output_file_included)
        os.remove(local_output_file_excluded)

        # check files were pushed to CyVerse
        files = list_files(remote_path, token)
        assert len(files) == 1
        assert join(remote_path, 'included.output') in [file['path'] for file in files]
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)
