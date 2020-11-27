import tempfile
from os import remove
from os.path import join, isfile

from plantit_cli.executor.executor import Executor
from plantit_cli.run import Run
from plantit_cli.tests.test_utils import clear_dir, check_hello

message = "Message!"
testdir = '/opt/plantit-cli/runs/'
tempdir = tempfile.gettempdir()


def test_run_with_params(mock_store):
    try:
        # expect 1 container
        Executor(mock_store).execute(Run(
            identifier='run_with_params',
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
            ]))

        # check local file was written
        file = join(testdir, 'message.txt')
        assert isfile(file)
        with open(file) as file:
            lines = file.readlines()
            assert len(lines) == 1
            assert lines[0] == f"{message}\n"
    finally:
        clear_dir(testdir)
        mock_store.clear()


def test_run_with_single_file_input(
        mock_store,
        remote_base_path,
        file_name_1):
    local_path = join(testdir, file_name_1)
    remote_path = join(remote_base_path[1:], "testCollection")

    try:
        # prep file
        with open(local_path, "w") as file1:
            file1.write('Hello, 1!')
        mock_store.upload_file(local_path, remote_path)

        # expect 1 container
        Executor(mock_store).execute(Run(
            identifier='run_with_file_input',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat "$INPUT" | tee "$INPUT.output"',
            input={
                'kind': 'file',
                'from': join(remote_path, file_name_1),
            }))

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
        mock_store.clear()


def test_run_with_directory_input_many_files(
        mock_store,
        remote_base_path,
        file_name_1,
        file_name_2):
    local_path_1 = join(testdir, file_name_1)
    local_path_2 = join(testdir, file_name_2)
    remote_path = join(remote_base_path[1:], "testCollection")

    try:
        # prep files
        with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')
        mock_store.upload_file(local_path_1, remote_path)
        mock_store.upload_file(local_path_2, remote_path)

        # expect 2 containers
        Executor(mock_store).execute(Run(
            identifier='run_with_directory_input',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat $INPUT | tee $INPUT.output',
            input={
                'kind': 'directory',
                'many': True,
                'from': remote_path,
            }))

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
        mock_store.clear()


def test_run_with_file_output(
        mock_store,
        remote_base_path):
    local_output_path = join(testdir, 'output.txt')
    remote_path = join(remote_base_path[1:], "testCollection")

    try:
        # expect 1 container
        Executor(mock_store).execute(Run(
            identifier='run_with_file_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='echo "Hello, world!" >> $OUTPUT',
            output={
                'to': remote_path,
                'from': 'output.txt',
            }))

        # check files were written locally
        assert isfile(local_output_path)
        check_hello(local_output_path, 'world')
        # os.remove(local_output_file)

        # check file was pushed
        files = mock_store.list_directory(remote_path)
        assert join(mock_store.dir, remote_path, 'output.txt') in files
    finally:
        clear_dir(testdir)
        mock_store.clear()


def test_run_with_directory_output(
        mock_store,
        remote_base_path):
    local_output_path = testdir
    local_output_file_1 = join(local_output_path, 't1.txt')
    local_output_file_2 = join(local_output_path, 't2.txt')
    remote_path = join(remote_base_path[1:], "testCollection")

    try:
        # expect 1 container
        Executor(mock_store).execute(Run(
            identifier='run_with_directory_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='echo "Hello, world!" | tee $OUTPUT/t1.txt $OUTPUT/t2.txt',
            output={
                'to': remote_path,
                'from': '',
            }))

        # check files were written locally
        assert isfile(local_output_file_1)
        assert isfile(local_output_file_2)
        check_hello(local_output_file_1, 'world')
        check_hello(local_output_file_2, 'world')
        remove(local_output_file_1)
        remove(local_output_file_2)

        # check files were pushed
        files = mock_store.list_directory(remote_path)
        assert join(mock_store.dir, remote_path, 't1.txt') in files
        assert join(mock_store.dir, remote_path, 't2.txt') in files
    finally:
        clear_dir(testdir)
        mock_store.clear()


def test_run_with_file_input_and_directory_output(
        mock_store,
        remote_base_path,
        file_name_1):
    local_input_file_path = join(testdir, file_name_1)
    local_output_path = join(testdir, 'input') # write output files to input dir
    local_output_file_path = join(local_output_path, f"{file_name_1}.output")
    remote_path = join(remote_base_path[1:], "testCollection")

    try:
        # prep file
        with open(local_input_file_path, "w") as file1:
            file1.write('Hello, 1!')
        mock_store.upload_file(local_input_file_path, remote_path)

        # expect 1 container
        Executor(mock_store).execute(Run(
            identifier='run_with_file_input_and_directory_output',
            workdir=testdir,
            image="docker://alpine:latest",
            command='cat $INPUT | tee $INPUT.output',
            input={
                'kind': 'file',
                'from': join(remote_path, file_name_1),
            },
            output={
                'to': remote_path,
                'from': 'input', # write output files to input dir
                'pattern': 'output'
            }))

        # check file was written locally
        assert isfile(local_output_file_path)
        check_hello(local_output_file_path, '1')
        remove(local_output_file_path)

        # check file was pushed to CyVerse
        files = mock_store.list_directory(remote_path)
        assert join(mock_store.dir, remote_path, f"{file_name_1}.output") in files
    finally:
        clear_dir(testdir)
        mock_store.clear()


def test_run_with_directory_output_with_excludes(
        mock_store,
        remote_base_path):
    local_output_path = testdir
    local_output_file_included = join(local_output_path, "included.output")
    local_output_file_excluded = join(local_output_path, "excluded.output")
    remote_path = join(remote_base_path[1:], "testCollection")

    try:
        # expect 1 container
        Executor(mock_store).execute(Run(
            identifier='run_with_directory_output_with_excludes',
            workdir=testdir,
            image="docker://alpine:latest",
            command='touch excluded.output && touch included.output',
            output={
                'to': remote_path,
                'from': '',
                'pattern': 'output',
                'exclude': [
                    'excluded.output'
                ]
            }))

        # check files were written locally
        assert isfile(local_output_file_included)
        assert isfile(local_output_file_excluded)
        remove(local_output_file_included)
        remove(local_output_file_excluded)

        # check files were pushed to CyVerse
        files = mock_store.list_directory(remote_path)
        assert len(files) == 1
        assert join(mock_store.dir, remote_path, 'included.output') in files
    finally:
        clear_dir(testdir)
        mock_store.clear()