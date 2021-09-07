import os
import tempfile
from os import remove, environ
from os.path import join

import pytest

from plantit_cli import commands
from plantit_cli.store import terrain_commands
from plantit_cli.tests.integration.terrain_test_utils import create_collection, upload_file, delete_collection
from plantit_cli.tests.utils import clear_dir, check_hello, Token

message = "Message"
testdir = '/opt/plantit-cli/runs'
tempdir = tempfile.gettempdir()
token = '' # Token.get()
DEFAULT_SLEEP = 45


@pytest.mark.skip(reason='debug')
def test_pull_then_run_file_input(remote_base_path, file_name_1):
    local_path = join(testdir, file_name_1)
    remote_path = join(remote_base_path, "testCollection")
    options = {
        'workdir': testdir,
        'image': "docker://alpine:latest",
        'command': 'cat "$INPUT" > "$INPUT.output"',
        'input': {'path': join(testdir, file_name_1), 'kind': 'file'}
    }

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_path, "w") as file1:
            file1.write('Hello, 1!')
        upload_file(local_path, remote_path, token)
        remove(local_path)

        # pull file to test directory
        terrain_commands.pull(remote_path, testdir, cyverse_token=token)

        # check file was pulled
        downloaded_path = join(testdir, file_name_1)
        check_hello(downloaded_path, 1)
        remove(downloaded_path)

        # expect 1 container
        commands.run(
            options=options,
            docker_username=environ.get('DOCKER_USERNAME', None),
            docker_password=environ.get('DOCKER_PASSWORD', None))

        # check local output file was written
        output_1 = f"{downloaded_path}.output"
        check_hello(output_1, 1)
        remove(output_1)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


@pytest.mark.skip(reason='debug')
def test_pull_then_run_file_input_and_parameters(remote_base_path, file_name_1):
    local_path = join(testdir, file_name_1)
    remote_path = join(remote_base_path, "testCollection")
    options = {
        'workdir': testdir,
        'image': "docker://alpine:latest",
        'command': 'cat $INPUT > $INPUT.$TAG.output',
        'input': {'path': local_path, 'kind': 'file'},
        'parameters': [{'key': 'TAG', 'value': message}]
    }

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # prep file
        with open(local_path, "w") as file1:
            file1.write('Hello, 1!')
        upload_file(local_path, remote_path, token)
        os.remove(local_path)

        # pull file to test directory
        terrain_commands.pull(remote_path, testdir, cyverse_token=token)

        # check file was pulled
        check_hello(local_path, 1)
        remove(local_path)

        # expect 1 container
        commands.run(
            options=options,
            docker_username=environ.get('DOCKER_USERNAME', None),
            docker_password=environ.get('DOCKER_PASSWORD', None))

        # check local output file was written
        output_1 = f"{local_path}.{message}.output"
        check_hello(output_1, 1)
        remove(output_1)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)

# def test_run_fails_with_no_params_and_file_input_and_no_output_when_no_inputs_found(remote_base_path, file_name_1):
#    time.sleep(DEFAULT_SLEEP)
#    plan = RunOptions(
#        identifier='test_run_fails_with_no_params_and_file_input_and_no_output_when_no_inputs_found',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='cat "$INPUT" | tee "$INPUT.output"',
#        input={
#            'kind': 'file',
#            'from': join(remote_base_path, "testCollection", file_name_1),
#        },
#        cyverse_token=token)
#
#    # expect exception
#    with pytest.raises(PlantitException):
#        Runner(TerrainStore(plan)).run(plan)
#    time.sleep(DEFAULT_SLEEP)
#
#
# def test_run_fails_with_params_and_file_input_and_no_output_when_no_inputs_found(remote_base_path,
#                                                                                 file_name_1):
#    time.sleep(DEFAULT_SLEEP)
#    plan = RunOptions(
#        identifier='test_run_fails_with_params_and_file_input_and_no_output_when_no_inputs_found',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='cat "$INPUT" | tee "$INPUT.$TAG.output"',
#        input={
#            'kind': 'file',
#            'from': join(remote_base_path, "testCollection", file_name_1),
#        },
#        cyverse_token=token,
#        parameters=[
#            {
#                'key': 'TAG',
#                'value': message
#            },
#        ])
#    # expect exception
#    with pytest.raises(PlantitException):
#        Runner(TerrainStore(plan)).run(plan)
#    time.sleep(DEFAULT_SLEEP)
#
#
# def test_run_succeeds_with_no_params_and_files_input_and_no_output(
#        remote_base_path,
#        file_name_1,
#        file_name_2):
#    local_path_1 = join(testdir, file_name_1)
#    local_path_2 = join(testdir, file_name_2)
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_no_params_and_files_input_and_no_output',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='cat $INPUT | tee $INPUT.output',
#        input={
#            'kind': 'files',
#            'from': join(remote_base_path, "testCollection"),
#        },
#        cyverse_token=token)
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # prep files
#        with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
#            file1.write('Hello, 1!')
#            file2.write('Hello, 2!')
#        upload_file(local_path_1, remote_path, token)
#        upload_file(local_path_2, remote_path, token)
#
#        # expect 2 containers
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check files were pulled
#        downloaded_path_1 = join(testdir, 'input', file_name_1)
#        downloaded_path_2 = join(testdir, 'input', file_name_2)
#        check_hello(downloaded_path_1, 1)
#        check_hello(downloaded_path_2, 2)
#        remove(downloaded_path_1)
#        remove(downloaded_path_2)
#
#        # check local output files were written
#        output_1 = f"{downloaded_path_1}.output"
#        output_2 = f"{downloaded_path_2}.output"
#        check_hello(output_1, 1)
#        check_hello(output_2, 2)
#        remove(output_1)
#        remove(output_2)
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_no_params_and_files_input_and_patterns_and_no_output(
#        remote_base_path,
#        file_name_1,
#        file_name_2):
#    local_path_1 = join(testdir, file_name_1)
#    local_path_2 = join(testdir, file_name_2)
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_no_params_and_files_input_and_no_output',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='cat $INPUT | tee $INPUT.output',
#        input={
#            'kind': 'files',
#            'from': join(remote_base_path, "testCollection"),
#            'patterns': [
#                file_name_1
#            ]
#        },
#        cyverse_token=token)
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # prep files
#        with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
#            file1.write('Hello, 1!')
#            file2.write('Hello, 2!')
#        upload_file(local_path_1, remote_path, token)
#        upload_file(local_path_2, remote_path, token)
#
#        # expect 2 containers
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check files were pulled
#        downloaded_path_1 = join(testdir, 'input', file_name_1)
#        downloaded_path_2 = join(testdir, 'input', file_name_2)
#        assert not isfile(downloaded_path_2)
#        check_hello(downloaded_path_1, 1)
#        remove(downloaded_path_1)
#
#        # check local output files were written
#        output_1 = f"{downloaded_path_1}.output"
#        check_hello(output_1, 1)
#        remove(output_1)
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_params_and_files_input_and_no_output(
#        remote_base_path,
#        file_name_1,
#        file_name_2):
#    local_path_1 = join(testdir, file_name_1)
#    local_path_2 = join(testdir, file_name_2)
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_params_and_files_input_and_no_output',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='cat $INPUT | tee $INPUT.$TAG.output',
#        input={
#            'kind': 'files',
#            'from': join(remote_base_path, "testCollection"),
#        },
#        cyverse_token=token,
#        parameters=[
#            {
#                'key': 'TAG',
#                'value': message
#            },
#        ])
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # prep files
#        with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
#            file1.write('Hello, 1!')
#            file2.write('Hello, 2!')
#        upload_file(local_path_1, remote_path, token)
#        upload_file(local_path_2, remote_path, token)
#
#        # expect 2 containers
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check files were pulled
#        downloaded_path_1 = join(testdir, 'input', file_name_1)
#        downloaded_path_2 = join(testdir, 'input', file_name_2)
#        check_hello(downloaded_path_1, 1)
#        check_hello(downloaded_path_2, 2)
#        remove(downloaded_path_1)
#        remove(downloaded_path_2)
#
#        # check local output files were written
#        output_1 = f"{downloaded_path_1}.{message}.output"
#        output_2 = f"{downloaded_path_2}.{message}.output"
#        check_hello(output_1, 1)
#        check_hello(output_2, 2)
#        remove(output_1)
#        remove(output_2)
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_no_params_and_no_input_and_file_output(remote_base_path):
#    local_output_path = join(testdir, 'output.txt')
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_no_params_and_no_input_and_file_output',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='echo "Hello, world!" >> $OUTPUT',
#        output={
#            'to': join(remote_base_path, "testCollection"),
#            'from': 'output.txt',
#        },
#        cyverse_token=token)
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # run
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check files were written locally
#        assert isfile(local_output_path)
#        check_hello(local_output_path, 'world')
#        # os.remove(local_output_file)
#
#        # check file was pushed to CyVerse
#        files = list_files(remote_path, token)
#        assert join(remote_path, 'output.txt') in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_params_and_no_input_and_file_output(remote_base_path):
#    local_output_path = join(testdir, f"output.{message}.txt")
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_params_and_no_input_and_file_output',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='echo "Hello, world!" >> $OUTPUT',
#        output={
#            'to': join(remote_base_path, "testCollection"),
#            'from': f"output.{message}.txt",
#        },
#        cyverse_token=token,
#        parameters=[
#            {
#                'key': 'TAG',
#                'value': message
#            },
#        ])
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # run
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check files were written locally
#        assert isfile(local_output_path)
#        check_hello(local_output_path, 'world')
#        # os.remove(local_output_file)
#
#        # check file was pushed to CyVerse
#        files = list_files(remote_path, token)
#        assert join(remote_path, f"output.{message}.txt") in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_no_params_and_no_input_and_directory_output(remote_base_path):
#    local_output_path = testdir
#    local_output_file_1 = join(local_output_path, 't1.txt')
#    local_output_file_2 = join(local_output_path, 't2.txt')
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='echo "Hello, world!" | tee $OUTPUT/t1.txt $OUTPUT/t2.txt',
#        output={
#            'to': join(remote_base_path, "testCollection"),
#            'from': '',
#        },
#        cyverse_token=token)
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # run
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check files were written locally
#        assert isfile(local_output_file_1)
#        assert isfile(local_output_file_2)
#        check_hello(local_output_file_1, 'world')
#        check_hello(local_output_file_2, 'world')
#        remove(local_output_file_1)
#        remove(local_output_file_2)
#
#        # check files were pushed to CyVerse
#        files = list_files(remote_path, token)
#        assert join(remote_path, 't1.txt') in [file['path'] for file in files]
#        assert join(remote_path, 't2.txt') in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# @pytest.mark.skip(reason="fails on GitHub Actions build (why?)")
# def test_run_succeeds_with_params_and_no_input_and_directory_output(remote_base_path):
#    local_output_path = testdir
#    local_output_file_1 = join(local_output_path, f"t1.{message}.txt")
#    local_output_file_2 = join(local_output_path, f"t2.{message}.txt")
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_params_and_no_input_and_directory_output',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='echo "Hello, world!" | tee $OUTPUT/t1.$TAG.txt $OUTPUT/t2.$TAG.txt',
#        output={
#            'to': join(remote_base_path, "testCollection"),
#            'from': '',
#        },
#        cyverse_token=token,
#        parameters=[
#            {
#                'key': 'TAG',
#                'value': message
#            },
#        ])
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # run
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check files were written locally
#        assert isfile(local_output_file_1)
#        assert isfile(local_output_file_2)
#        check_hello(local_output_file_1, 'world')
#        check_hello(local_output_file_2, 'world')
#        remove(local_output_file_1)
#        remove(local_output_file_2)
#
#        # check files were pushed to CyVerse
#        files = list_files(remote_path, token)
#        assert join(remote_path, f"t1.{message}.txt") in [file['path'] for file in files]
#        assert join(remote_path, f"t2.{message}.txt") in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_no_params_and_file_input_and_directory_output(
#        remote_base_path,
#        file_name_1):
#    local_input_file_path = join(testdir, file_name_1)
#    local_output_path = join(testdir, 'input')  # write output files to input dir
#    local_output_file_path = join(local_output_path, f"{file_name_1}.output")
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_no_params_and_file_input_and_directory_output',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='cat $INPUT | tee $INPUT.output',
#        input={
#            'kind': 'file',
#            'from': join(remote_base_path, "testCollection", file_name_1),
#        },
#        output={
#            'to': join(remote_base_path, "testCollection"),
#            'from': 'input',  # write output files to input dir
#            'include': {'patterns': ['output'], 'names': []}
#        },
#        cyverse_token=token)
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # prep file
#        with open(local_input_file_path, "w") as file1:
#            file1.write('Hello, 1!')
#        upload_file(local_input_file_path, remote_path, token)
#
#        # expect 1 container
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check file was written locally
#        assert isfile(local_output_file_path)
#        check_hello(local_output_file_path, '1')
#        remove(local_output_file_path)
#
#        # check file was pushed to CyVerse
#        files = list_files(remote_path, token)
#        assert join(remote_path, f"{file_name_1}.output") in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_params_and_file_input_and_directory_output(
#        remote_base_path,
#        file_name_1):
#    local_input_file_path = join(testdir, file_name_1)
#    local_output_path = join(testdir, 'input')  # write output files to input dir
#    local_output_file_path = join(local_output_path, f"{file_name_1}.{message}.output")
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_params_and_file_input_and_directory_output',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='cat $INPUT | tee $INPUT.$TAG.output',
#        input={
#            'kind': 'file',
#            'from': join(remote_base_path, "testCollection", file_name_1),
#        },
#        output={
#            'to': join(remote_base_path, "testCollection"),
#            'from': 'input',  # write output files to input dir
#            'include': {'patterns': ['output'], 'names': []}
#        },
#        cyverse_token=token,
#        parameters=[
#            {
#                'key': 'TAG',
#                'value': message
#            },
#        ])
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # prep file
#        with open(local_input_file_path, "w") as file1:
#            file1.write('Hello, 1!')
#        upload_file(local_input_file_path, remote_path, token)
#
#        # expect 1 container
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check file was written locally
#        assert isfile(local_output_file_path)
#        check_hello(local_output_file_path, '1')
#        remove(local_output_file_path)
#
#        # check file was pushed to CyVerse
#        files = list_files(remote_path, token)
#        assert join(remote_path, f"{file_name_1}.{message}.output") in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_no_params_and_directory_input_and_directory_output(
#        remote_base_path,
#        file_name_1,
#        file_name_2):
#    local_input_file_path_1 = join(testdir, file_name_1)
#    local_input_file_path_2 = join(testdir, file_name_2)
#    remote_path = join(remote_base_path, "testCollection")
#    local_output_dir = join(testdir, 'input')  # write output files to input dir
#    local_output_path = join(local_output_dir, f"{join(testdir, 'input')}.output")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_no_params_and_directory_input_and_directory_output',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='ls $INPUT | tee $INPUT.output',
#        input={
#            'kind': 'directory',
#            'from': remote_path,
#        },
#        output={
#            'to': remote_path,
#            'from': '',
#            'include': {'patterns': ['output'], 'names': []}
#        },
#        cyverse_token=token)
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # prep file
#        with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
#            file1.write('Hello, 1!')
#            file2.write('Hello, 2!')
#        upload_file(local_input_file_path_1, remote_path, token)
#        upload_file(local_input_file_path_2, remote_path, token)
#
#        # expect 1 container
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check file was written locally
#        assert isfile(local_output_path)
#        with open(local_output_path) as file:
#            lines = file.readlines()
#            assert len(lines) == 2
#            assert local_input_file_path_1.split('/')[-1] in lines[0]
#            assert local_input_file_path_2.split('/')[-1] in lines[1]
#        remove(local_output_path)
#
#        # check file was pushed to store
#        files = list_files(remote_path, token)
#        assert join(remote_path, local_output_path.split('/')[-1]) in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_params_and_directory_input_and_directory_output(
#        remote_base_path,
#        file_name_1,
#        file_name_2):
#    local_input_file_path_1 = join(testdir, file_name_1)
#    local_input_file_path_2 = join(testdir, file_name_2)
#    remote_path = join(remote_base_path, "testCollection")
#    local_output_dir = join(testdir, 'input')  # write output files to input dir
#    local_output_path = join(local_output_dir, f"{join(testdir, 'input')}.{message}.output")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_params_and_directory_input_and_directory_output',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='ls $INPUT | tee $INPUT.$TAG.output',
#        input={
#            'kind': 'directory',
#            'from': remote_path,
#        },
#        output={
#            'to': remote_path,
#            'from': '',
#            'include': {'patterns': ['output'], 'names': []}
#        },
#        parameters=[
#            {
#                'key': 'TAG',
#                'value': message
#            },
#        ],
#        cyverse_token=token)
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # prep file
#        with open(local_input_file_path_1, "w") as file1, open(local_input_file_path_2, "w") as file2:
#            file1.write('Hello, 1!')
#            file2.write('Hello, 2!')
#        upload_file(local_input_file_path_1, remote_path, token)
#        upload_file(local_input_file_path_2, remote_path, token)
#
#        # expect 1 container
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check file was written locally
#        assert isfile(local_output_path)
#        with open(local_output_path) as file:
#            lines = file.readlines()
#            assert len(lines) == 2
#            assert local_input_file_path_1.split('/')[-1] in lines[0]
#            assert local_input_file_path_2.split('/')[-1] in lines[1]
#        remove(local_output_path)
#
#        # check file was pushed to store
#        files = list_files(remote_path, token)
#        assert join(remote_path, local_output_path.split('/')[-1]) in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_excludes(remote_base_path):
#    local_output_path = testdir
#    local_output_file_included = join(local_output_path, "included.output")
#    local_output_file_excluded = join(local_output_path, "excluded.output")
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_excludes',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='touch excluded.output included.output',
#        output={
#            'to': join(remote_base_path, "testCollection"),
#            'from': '',
#            'include': {'patterns': ['output'], 'names': []},
#            'exclude': {'patterns': [], 'names': [
#                'excluded.output'
#            ]}
#        },
#        cyverse_token=token)
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # expect 1 container
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check files were written locally
#        assert isfile(local_output_file_included)
#        assert isfile(local_output_file_excluded)
#        remove(local_output_file_included)
#        remove(local_output_file_excluded)
#
#        # check files were pushed to CyVerse
#        files = list_files(remote_path, token)
#        assert len(files) == 1
#        assert join(remote_path, 'included.output') in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_params_and_no_input_and_directory_output_with_excludes(remote_base_path):
#    local_output_path = testdir
#    local_output_file_included = join(local_output_path, f"included.{message}.output")
#    local_output_file_excluded = join(local_output_path, "excluded.output")
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_params_and_no_input_and_directory_output_with_excludes',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='touch excluded.output included.$TAG.output',
#        output={
#            'to': join(remote_base_path, "testCollection"),
#            'from': '',
#            'include': {'patterns': ['output'], 'names': []},
#            'exclude': {'patterns': [], 'names': [
#                'excluded.output'
#            ]}
#        },
#        cyverse_token=token,
#        parameters=[
#            {
#                'key': 'TAG',
#                'value': message
#            },
#        ])
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # expect 1 container
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check files were written locally
#        assert isfile(local_output_file_included)
#        assert isfile(local_output_file_excluded)
#        remove(local_output_file_included)
#        remove(local_output_file_excluded)
#
#        # check files were pushed to CyVerse
#        files = list_files(remote_path, token)
#        assert len(files) == 1
#        assert join(remote_path, f"included.{message}.output") in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes(
#        remote_base_path):
#    local_output_path = testdir
#    local_output_file_included = join(local_output_path, "included.output")
#    local_output_file_excluded = join(local_output_path, "excluded.output")
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_no_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='touch excluded.output included.output',
#        output={
#            'to': join(remote_base_path, "testCollection"),
#            'from': '',
#            'include': {'patterns': ['output'], 'names': []},
#            'exclude': {'patterns': [], 'names': [
#                'excluded.output'
#            ]}
#        },
#        cyverse_token=token)
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # expect 1 container
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check files were written locally
#        assert isfile(local_output_file_included)
#        assert isfile(local_output_file_excluded)
#        remove(local_output_file_included)
#        remove(local_output_file_excluded)
#
#        # check files were pushed to CyVerse
#        files = list_files(remote_path, token)
#        assert len(files) == 1
#        assert join(remote_path, 'included.output') in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes(
#        remote_base_path):
#    local_output_path = testdir
#    local_output_file_included = join(local_output_path, f"included.{message}.output")
#    local_output_file_excluded = join(local_output_path, "excluded.output")
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_params_and_no_input_and_directory_output_with_non_matching_case_pattern_and_excludes',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='touch excluded.output included.$TAG.output',
#        output={
#            'to': join(remote_base_path, "testCollection"),
#            'from': '',
#            'include': {'patterns': ['OUTPUT'], 'names': []},
#            'exclude': {'patterns': [], 'names': [
#                'excluded.output'
#            ]}
#        },
#        cyverse_token=token,
#        parameters=[
#            {
#                'key': 'TAG',
#                'value': message
#            },
#        ])
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # expect 1 container
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check files were written locally
#        assert isfile(local_output_file_included)
#        assert isfile(local_output_file_excluded)
#        remove(local_output_file_included)
#        remove(local_output_file_excluded)
#
#        # check files were pushed to CyVerse
#        files = list_files(remote_path, token)
#        assert len(files) == 1
#        assert join(remote_path, f"included.{message}.output") in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
#
#
# def test_run_succeeds_with_params_and_no_input_and_directory_output_with_already_existing_output(remote_base_path, file_name_1):
#    local_output_path = testdir
#    local_input_file_path_1 = join(testdir, file_name_1)
#    local_output_file_included = join(local_output_path, f"included.{message}.output")
#    local_output_file_excluded = join(local_output_path, "excluded.output")
#    remote_path = join(remote_base_path, "testCollection")
#    plan = RunOptions(
#        identifier='test_run_succeeds_with_params_and_no_input_and_directory_output_with_excludes',
#        workdir=testdir,
#        image="docker://alpine:latest",
#        command='touch excluded.output included.$TAG.output',
#        output={
#            'to': join(remote_base_path, "testCollection"),
#            'from': '',
#            'include': {'patterns': ['output', file_name_1], 'names': []},
#            'exclude': {'patterns': [], 'names': [
#                'excluded.output'
#            ]}
#        },
#        cyverse_token=token,
#        parameters=[
#            {
#                'key': 'TAG',
#                'value': message
#            },
#        ])
#
#    try:
#        # prep CyVerse collection
#        create_collection(remote_path, token)
#
#        # prep file
#        with open(local_input_file_path_1, "w") as file1:
#            file1.write('Hello, 1!')
#        upload_file(local_input_file_path_1, remote_path, token)
#
#        # expect 1 container
#        Runner(TerrainStore(plan)).run(plan)
#
#        # check files were written locally
#        assert isfile(local_output_file_included)
#        assert isfile(local_output_file_excluded)
#        remove(local_output_file_included)
#        remove(local_output_file_excluded)
#
#        # check files were pushed to CyVerse
#        files = list_files(remote_path, token)
#        assert len(files) == 2
#        assert join(remote_path, f"included.{message}.output") in [file['path'] for file in files]
#    finally:
#        clear_dir(testdir)
#        delete_collection(remote_path, token)
