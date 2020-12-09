from os import remove, environ
from os.path import join, isfile
from tempfile import TemporaryDirectory

import pytest

from plantit_cli.exceptions import PlantitException
from plantit_cli.runner.runner import Runner
from plantit_cli.plan import Plan
from plantit_cli.store.local_store import LocalStore
from plantit_cli.tests.test_utils import clear_dir, check_hello

message = "Message"
testdir = environ.get('TEST_DIRECTORY')


def test_run_succeeds_with_params_and_no_input_and_no_output():
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        try:
            # expect 1 container
            Runner(local_store).run(Plan(
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


def test_run_succeeds_with_no_params_and_file_input_and_no_output(remote_base_path, file_name_1):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_path = join(testdir, file_name_1)
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # prep file
            with open(local_path, "w") as file1:
                file1.write('Hello, 1!')
            local_store.upload_file(local_path, remote_path)

            # expect 1 container
            Runner(local_store).run(Plan(
                identifier='test_run_succeeds_with_no_params_and_file_input_and_no_output',
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


def test_run_succeeds_with_params_and_file_input_and_no_output(remote_base_path, file_name_1):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_path = join(testdir, file_name_1)
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # prep file
            with open(local_path, "w") as file1:
                file1.write('Hello, 1!')
            local_store.upload_file(local_path, remote_path)

            # expect 1 container
            Runner(local_store).run(Plan(
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
                ]))

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
        local_store = LocalStore(temp_dir)
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect exception
            with pytest.raises(FileNotFoundError):
                Runner(local_store).run(Plan(
                    identifier='test_run_fails_with_no_params_and_file_input_and_no_output_when_no_inputs_found',
                    workdir=testdir,
                    image="docker://alpine:latest",
                    command='cat "$INPUT" | tee "$INPUT.output"',
                    input={
                        'kind': 'file',
                        'from': join(remote_path, file_name_1),
                    }))
        finally:
            clear_dir(testdir)


def test_run_fails_with_params_and_file_input_and_no_output_when_no_inputs_found(remote_base_path, file_name_1):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect exception
            with pytest.raises(FileNotFoundError):
                Runner(local_store).run(Plan(
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
                    ]))
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_files_input_and_no_output(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_path_1 = join(testdir, file_name_1)
        local_path_2 = join(testdir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # prep files
            with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            local_store.upload_file(local_path_1, remote_path)
            local_store.upload_file(local_path_2, remote_path)

            # expect 2 containers
            Runner(local_store).run(Plan(
                identifier='test_run_succeeds_with_no_params_and_files_input_and_no_output',
                workdir=testdir,
                image="docker://alpine:latest",
                command='cat $INPUT | tee $INPUT.output',
                input={
                    'kind': 'files',
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


def test_run_succeeds_with_params_and_files_input_and_no_output(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_path_1 = join(testdir, file_name_1)
        local_path_2 = join(testdir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # prep files
            with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            local_store.upload_file(local_path_1, remote_path)
            local_store.upload_file(local_path_2, remote_path)

            # expect 2 containers
            Runner(local_store).run(Plan(
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
                ]))

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
        local_store = LocalStore(temp_dir)
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect exception
            with pytest.raises(PlantitException):
                Runner(local_store).run(Plan(
                    identifier='test_run_fails_with_no_params_and_files_input_and_no_output_when_no_inputs_found',
                    workdir=testdir,
                    image="docker://alpine:latest",
                    command='cat $INPUT | tee $INPUT.output',
                    input={
                        'kind': 'files',
                        'from': remote_path,
                    }))
        finally:
            clear_dir(testdir)


def test_run_fails_with_params_and_files_input_and_no_output_when_no_inputs_found(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect exception
            with pytest.raises(PlantitException):
                Runner(local_store).run(Plan(
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
                    ]))
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_no_input_and_file_output(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_output_path = join(testdir, 'output.txt')
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect 1 container
            Runner(local_store).run(Plan(
                identifier='test_run_succeeds_with_no_params_and_no_input_and_file_output',
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
            files = local_store.list_directory(remote_path)
            assert join(local_store.dir, remote_path, 'output.txt') in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_no_input_and_file_output(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_output_path = join(testdir, f"output.{message}.txt")
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect 1 container
            Runner(local_store).run(Plan(
                identifier='test_run_succeeds_with_params_and_no_input_and_file_output',
                workdir=testdir,
                image="docker://alpine:latest",
                command='echo "Hello, world!" >> $OUTPUT',
                output={
                    'to': remote_path,
                    'from': f"output.{message}.txt",
                },
                params=[
                    {
                        'key': 'TAG',
                        'value': message
                    },
                ]))

            # check files were written locally
            assert isfile(local_output_path)
            check_hello(local_output_path, 'world')
            # os.remove(local_output_file)

            # check file was pushed
            files = local_store.list_directory(remote_path)
            assert join(local_store.dir, remote_path, f"output.{message}.txt") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_no_input_and_directory_output(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_output_path = testdir
        local_output_file_1 = join(local_output_path, 't1.txt')
        local_output_file_2 = join(local_output_path, 't2.txt')
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect 1 container
            Runner(local_store).run(Plan(
                identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output',
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
            files = local_store.list_directory(remote_path)
            assert join(local_store.dir, remote_path, 't1.txt') in files
            assert join(local_store.dir, remote_path, 't2.txt') in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_no_input_and_directory_output(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_output_path = testdir
        local_output_file_1 = join(local_output_path, f"t1.{message}.txt")
        local_output_file_2 = join(local_output_path, f"t2.{message}.txt")
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect 1 container
            Runner(local_store).run(Plan(
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
                ]))

            # check files were written locally
            assert isfile(local_output_file_1)
            assert isfile(local_output_file_2)
            check_hello(local_output_file_1, 'world')
            check_hello(local_output_file_2, 'world')
            remove(local_output_file_1)
            remove(local_output_file_2)

            # check files were pushed
            files = local_store.list_directory(remote_path)
            assert join(local_store.dir, remote_path, f"t1.{message}.txt") in files
            assert join(local_store.dir, remote_path, f"t2.{message}.txt") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_file_input_and_directory_output(remote_base_path, file_name_1):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_input_file_path = join(testdir, file_name_1)
        local_output_path = join(testdir, 'input')  # write output files to input dir
        local_output_file_path = join(local_output_path, f"{file_name_1}.output")
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # prep file
            with open(local_input_file_path, "w") as file1:
                file1.write('Hello, 1!')
            local_store.upload_file(local_input_file_path, remote_path)

            # expect 1 container
            Runner(local_store).run(Plan(
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
                    'pattern': 'output'
                }))

            # check file was written locally
            assert isfile(local_output_file_path)
            check_hello(local_output_file_path, '1')
            remove(local_output_file_path)

            # check file was pushed to store
            files = local_store.list_directory(remote_path)
            assert join(local_store.dir, remote_path, f"{file_name_1}.output") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_file_input_and_directory_output(remote_base_path, file_name_1):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_input_file_path = join(testdir, file_name_1)
        local_output_path = join(testdir, 'input')  # write output files to input dir
        local_output_file_path = join(local_output_path, f"{file_name_1}.{message}.output")
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # prep file
            with open(local_input_file_path, "w") as file1:
                file1.write('Hello, 1!')
            local_store.upload_file(local_input_file_path, remote_path)

            # expect 1 container
            Runner(local_store).run(Plan(
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
                    'pattern': 'output'
                },
                params=[
                    {
                        'key': 'TAG',
                        'value': message
                    },
                ]))

            # check file was written locally
            assert isfile(local_output_file_path)
            check_hello(local_output_file_path, '1')
            remove(local_output_file_path)

            # check file was pushed to store
            files = local_store.list_directory(remote_path)
            assert join(local_store.dir, remote_path, f"{file_name_1}.{message}.output") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_files_input_and_directory_output(remote_base_path,
                                                                          file_name_1,
                                                                          file_name_2):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_input_file_path_1 = join(testdir, file_name_1)
        local_input_file_path_2 = join(testdir, file_name_2)
        local_output_path = join(testdir, 'input')  # write output files to input dir
        local_output_file_path_1 = join(local_output_path, f"{file_name_1}.output")
        local_output_file_path_2 = join(local_output_path, f"{file_name_2}.output")
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # prep file
            with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            local_store.upload_file(local_input_file_path_1, remote_path)
            local_store.upload_file(local_input_file_path_2, remote_path)

            # expect 2 containers
            Runner(local_store).run(Plan(
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
                    'pattern': 'output'
                }))

            # check files were written locally
            assert isfile(local_output_file_path_1)
            assert isfile(local_output_file_path_2)
            check_hello(local_output_file_path_1, '1')
            check_hello(local_output_file_path_2, '2')
            remove(local_output_file_path_1)
            remove(local_output_file_path_2)

            # check file was pushed to store
            files = local_store.list_directory(remote_path)
            assert join(local_store.dir, remote_path, f"{file_name_1}.output") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_files_input_and_directory_output(remote_base_path,
                                                                       file_name_1,
                                                                       file_name_2):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_input_file_path_1 = join(testdir, file_name_1)
        local_input_file_path_2 = join(testdir, file_name_2)
        local_output_path = join(testdir, 'input')  # write output files to input dir
        local_output_file_path_1 = join(local_output_path, f"{file_name_1}.{message}.output")
        local_output_file_path_2 = join(local_output_path, f"{file_name_2}.{message}.output")
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # prep file
            with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            local_store.upload_file(local_input_file_path_1, remote_path)
            local_store.upload_file(local_input_file_path_2, remote_path)

            # expect 2 containers
            Runner(local_store).run(Plan(
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
                    'pattern': 'output'
                },
                params=[
                    {
                        'key': 'TAG',
                        'value': message
                    },
                ]))

            # check files were written locally
            assert isfile(local_output_file_path_1)
            assert isfile(local_output_file_path_2)
            check_hello(local_output_file_path_1, '1')
            check_hello(local_output_file_path_2, '2')
            remove(local_output_file_path_1)
            remove(local_output_file_path_2)

            # check file was pushed to store
            files = local_store.list_directory(remote_path)
            assert join(local_store.dir, remote_path, f"{file_name_1}.{message}.output") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_directory_input_and_directory_output(remote_base_path,
                                                                              file_name_1,
                                                                              file_name_2):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_input_file_path_1 = join(testdir, file_name_1)
        local_input_file_path_2 = join(testdir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")
        local_output_path = join(local_store.dir, f"{join(testdir, 'input')}.output")

        try:
            # prep file
            with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            local_store.upload_file(local_input_file_path_1, remote_path)
            local_store.upload_file(local_input_file_path_2, remote_path)

            # expect 1 container
            Runner(local_store).run(Plan(
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
                    'pattern': 'output'
                }))

            # check file was written locally
            assert isfile(local_output_path)
            with open(local_output_path) as file:
                lines = file.readlines()
                assert len(lines) == 2
                assert local_input_file_path_1.split('/')[-1] in lines[0]
                assert local_input_file_path_2.split('/')[-1] in lines[1]
            remove(local_output_path)

            # check file was pushed to store
            files = local_store.list_directory(remote_path)
            assert join(local_store.dir, remote_path, local_output_path.split('/')[-1]) in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_directory_input_and_directory_output(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_input_file_path_1 = join(testdir, file_name_1)
        local_input_file_path_2 = join(testdir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")
        local_output_path = join(local_store.dir, f"{join(testdir, 'input')}.{message}.output")

        try:
            # prep file
            with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            local_store.upload_file(local_input_file_path_1, remote_path)
            local_store.upload_file(local_input_file_path_2, remote_path)

            # expect 1 container
            Runner(local_store).run(Plan(
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
                    'pattern': 'output'
                },
                params=[
                    {
                        'key': 'TAG',
                        'value': message
                    },
                ]))

            # check file was written locally
            assert isfile(local_output_path)
            with open(local_output_path) as file:
                lines = file.readlines()
                assert len(lines) == 2
                assert local_input_file_path_1.split('/')[-1] in lines[0]
                assert local_input_file_path_2.split('/')[-1] in lines[1]
            remove(local_output_path)

            # check file was pushed to store
            files = local_store.list_directory(remote_path)
            assert join(local_store.dir, remote_path, local_output_path.split('/')[-1]) in files
        finally:
            clear_dir(testdir)


def test_run_fails_with_no_params_and_directory_input_and_directory_output_when_no_inputs_found(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect exception
            with pytest.raises(PlantitException):
                Runner(local_store).run(Plan(
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
                        'pattern': 'output'
                    }))
        finally:
            clear_dir(testdir)


def test_run_fails_with_params_and_directory_input_and_directory_output_when_no_inputs_found(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect exception
            with pytest.raises(PlantitException):
                Runner(local_store).run(Plan(
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
                        'pattern': 'output'
                    },
                    params=[
                        {
                            'key': 'TAG',
                            'value': message
                        },
                    ]))
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_excludes(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_output_path = testdir
        local_output_file_included = join(local_output_path, "included.output")
        local_output_file_excluded = join(local_output_path, "excluded.output")
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect 1 container
            Runner(local_store).run(Plan(
                identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_excludes',
                workdir=testdir,
                image="docker://alpine:latest",
                command='touch excluded.output included.output',
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

            # check files were pushed to store
            files = local_store.list_directory(remote_path)
            assert len(files) == 1
            assert join(local_store.dir, remote_path, 'included.output') in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_no_input_and_directory_output_with_excludes(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_output_path = testdir
        local_output_file_included = join(local_output_path, f"included.{message}.output")
        local_output_file_excluded = join(local_output_path, "excluded.output")
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect 1 container
            Runner(local_store).run(Plan(
                identifier='test_run_succeeds_with_params_and_no_input_and_directory_output_with_excludes',
                workdir=testdir,
                image="docker://alpine:latest",
                command='touch excluded.output included.$TAG.output',
                output={
                    'to': remote_path,
                    'from': '',
                    'pattern': 'output',
                    'exclude': [
                        'excluded.output'
                    ]
                },
                params=[
                    {
                        'key': 'TAG',
                        'value': message
                    },
                ]))

            # check files were written locally
            assert isfile(local_output_file_included)
            assert isfile(local_output_file_excluded)
            remove(local_output_file_included)
            remove(local_output_file_excluded)

            # check files were pushed to store
            files = local_store.list_directory(remote_path)
            assert len(files) == 1
            assert join(local_store.dir, remote_path, f"included.{message}.output") in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_output_path = testdir
        local_output_file_included = join(local_output_path, "included.output")
        local_output_file_excluded = join(local_output_path, "excluded.output")
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect 1 container
            Runner(local_store).run(Plan(
                identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes',
                workdir=testdir,
                image="docker://alpine:latest",
                command='touch excluded.output included.output',
                output={
                    'to': remote_path,
                    'from': '',
                    'pattern': 'OUTPUT',
                    'exclude': [
                        'excluded.output'
                    ]
                }))

            # check files were written locally
            assert isfile(local_output_file_included)
            assert isfile(local_output_file_excluded)
            remove(local_output_file_included)
            remove(local_output_file_excluded)

            # check files were pushed to store
            files = local_store.list_directory(remote_path)
            assert len(files) == 1
            assert join(local_store.dir, remote_path, 'included.output') in files
        finally:
            clear_dir(testdir)


def test_run_succeeds_with_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes(remote_base_path):
    with TemporaryDirectory() as temp_dir:
        local_store = LocalStore(temp_dir)
        local_output_path = testdir
        local_output_file_included = join(local_output_path, f"included.{message}.output")
        local_output_file_excluded = join(local_output_path, "excluded.output")
        remote_path = join(remote_base_path[1:], "testCollection")

        try:
            # expect 1 container
            Runner(local_store).run(Plan(
                identifier='test_run_succeeds_with_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes',
                workdir=testdir,
                image="docker://alpine:latest",
                command='touch excluded.output included.$TAG.output',
                output={
                    'to': remote_path,
                    'from': '',
                    'pattern': 'OUTPUT',
                    'exclude': [
                        'excluded.output'
                    ]
                },
                params=[
                    {
                        'key': 'TAG',
                        'value': message
                    },
                ]))

            # check files were written locally
            assert isfile(local_output_file_included)
            assert isfile(local_output_file_excluded)
            remove(local_output_file_included)
            remove(local_output_file_excluded)

            # check files were pushed to store
            files = local_store.list_directory(remote_path)
            assert len(files) == 1
            assert join(local_store.dir, remote_path, f"included.{message}.output") in files
        finally:
            clear_dir(testdir)
