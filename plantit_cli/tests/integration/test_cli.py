import os
import tempfile
from os.path import join, isfile

import pytest
import requests
from click.testing import CliRunner

from plantit_cli.cli import run
from plantit_cli.tests.test_utils import clear_dir

from_path = f"/iplant/home/{os.environ.get('CYVERSE_USERNAME')}"
message = "Hello, world!"
testdir = '/opt/plantit-cli/runs'
tempdir = tempfile.gettempdir()
runner = CliRunner()


def test_run_cli_parameters(file_name_1, file_name_2):
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            input_file_path_1 = join(temp_dir, file_name_1)
            input_file_path_2 = join(temp_dir, file_name_2)
            with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # run the flow
            result = runner.invoke(run, ['../samples/flow_with_parameters.yaml'])
            print(result.output)
            assert result.exit_code == 0

            # check local message file
            file = join(testdir, 'output.txt')
            assert isfile(file)
            with open(file) as file:
                lines = file.readlines()
                assert len(lines) == 1
                assert lines[0] == f"{message}\n"
    except:
        clear_dir(testdir)
        raise
    finally:
        clear_dir(testdir)


def test_run_cli_bind_mounts(file_name_1, file_name_2):
    try:
        # run the flow
        result = runner.invoke(run, ['../samples/flow_with_bind_mounts.yaml'])
        print(result.output)
        assert result.exit_code == 0

        # check local message file
        file = join(testdir, 'output.txt')
        assert isfile(file)
        with open(file) as file:
            lines = file.readlines()
            assert len(lines) >= 1
            print(lines)
            assert any('flow_with_bind_mounts' in line for line in lines)
    except:
        clear_dir(testdir)
        raise
    finally:
        clear_dir(testdir)


# @pytest.mark.skip(reason='until fixed')
def test_run_cli_parameters_slurm(file_name_1, file_name_2):
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            input_file_path_1 = join(temp_dir, file_name_1)
            input_file_path_2 = join(temp_dir, file_name_2)
            with open(input_file_path_1, "w") as file1, open(input_file_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # run the flow
            result = runner.invoke(run, ['../samples/flow_with_parameters_slurm.yaml'])
            print(result.output)
            assert result.exit_code == 0

            # check local message file
            file = join(testdir, 'output.txt')
            assert isfile(file)
            with open(file) as file:
                lines = file.readlines()
                assert len(lines) == 1
                assert lines[0] == f"{message}\n"
    except:
        clear_dir(testdir)
        raise
    finally:
        clear_dir(testdir)


# def test_workflow_with_params_slurm():
#     try:
#         # run the workflow
#         os.environ['LC_ALL'] = 'C.UTF-8'
#         os.environ['LANG'] = 'C.UTF-8'
#         result = runner.invoke(run, ['examples/workflow_with_params_slurm.yaml'])
#         assert result.exit_code == 0
#
#         # check local message file
#         file = join(testdir, 'message.txt')
#         assert isfile(file)
#         with open(file) as file:
#             lines = file.readlines()
#             assert len(lines) == 1
#             assert lines[0] == f"{message}\n"
#     finally:
#         clear_dir(testdir)


# def test_workflow_with_file_input(cyverse_token):
#    local_file_1 = tempfile.NamedTemporaryFile()
#    local_file_2 = tempfile.NamedTemporaryFile()
#    local_file_1_name = local_file_1.name.split('/')[-1]
#    local_file_2_name = local_file_2.name.split('/')[-1]
#    collection = join(from_path, "testCollection")
#
#    try:
#        # prep iRODS files
#        session.collections.create(collection)
#        local_file_1.write(b'Hello, 1!')
#        local_file_1.seek(0)
#        local_file_2.write(b'Hello, 2!')
#        local_file_2.seek(0)
#        session.data_objects.put(local_file_1.name, join(collection, local_file_1_name))
#        session.data_objects.put(local_file_2.name, join(collection, local_file_2_name))
#        local_file_1.close()
#        local_file_2.close()
#
#        # run the workflow (expect 2 containers, 1 for each input file)
#        os.environ['LC_ALL'] = 'C.UTF-8'
#        os.environ['LANG'] = 'C.UTF-8'
#        result = runner.invoke(run, ['examples/workflow_with_file_input.yaml',
#                                     '--cyverse_token', cyverse_token])
#        assert result.exit_code == 0
#
#        # check input files were pulled from iRODS
#        input_1 = join(testdir, 'input', local_file_1_name)
#        input_2 = join(testdir, 'input', local_file_2_name)
#        check_hello(input_1, 1)
#        check_hello(input_2, 2)
#        os.remove(input_1)
#        os.remove(input_2)
#
#        # check local output files were written
#        output_1 = f"{input_1}.output"
#        output_2 = f"{input_2}.output"
#        check_hello(output_1, 1)
#        check_hello(output_2, 2)
#        os.remove(output_1)
#        os.remove(output_2)
#    finally:
#        clear_dir(testdir)
#        session.collections.remove(collection, force=True)
#
#
# def test_workflow_with_directory_input(cyverse_token):
#    local_file_1 = tempfile.NamedTemporaryFile()
#    local_file_2 = tempfile.NamedTemporaryFile()
#    local_path_1 = local_file_1.name
#    local_path_2 = local_file_2.name
#    local_name_1 = local_file_1.name.split('/')[-1]
#    local_name_2 = local_file_2.name.split('/')[-1]
#    collection = join(from_path, "testCollection")
#    remote_path_1 = join(collection, local_name_1)
#    remote_path_2 = join(collection, local_name_2)
#
#    try:
#        # prep iRODS collection
#        session.collections.create(collection)
#        local_file_1.write(b'Hello, 1!')
#        local_file_1.seek(0)
#        local_file_2.write(b'Hello, 2!')
#        local_file_2.seek(0)
#        session.data_objects.put(local_path_1, remote_path_1)
#        session.data_objects.put(local_path_2, remote_path_2)
#        local_file_1.close()
#        local_file_2.close()
#
#        # run the workflow (expect 1 container)
#        os.environ['LC_ALL'] = 'C.UTF-8'
#        os.environ['LANG'] = 'C.UTF-8'
#        result = runner.invoke(run, ['examples/workflow_with_directory_input.yaml',
#                                     '--cyverse_token', cyverse_token])
#        assert result.exit_code == 0
#
#        # check input files were pulled from iRODS
#        input_1 = join(testdir, 'input', local_name_1)
#        input_2 = join(testdir, 'input', local_name_2)
#        check_hello(input_1, 1)
#        check_hello(input_2, 2)
#        os.remove(input_1)
#        os.remove(input_2)
#
#        # check local output files were written
#        output = f"output.txt"
#        assert isfile(output)
#        with open(output, 'r') as file:
#            lines = [line.strip('\n') for line in file.readlines()]
#            assert len(lines) == 2
#            assert local_name_1 in lines
#            assert local_name_2 in lines
#        os.remove(output)
#    finally:
#        clear_dir(testdir)
#        session.collections.remove(collection, force=True)
#
#
# def test_workflow_with_file_output(cyverse_token, workflow_with_file_output):
#    from_path = join(testdir, workflow_with_file_output.output['from_path'])
#    collection = join(from_path, "testCollection")
#
#    try:
#        # prep iRODS collection
#        session.collections.create(collection)
#
#        # run the workflow
#        os.environ['LC_ALL'] = 'C.UTF-8'
#        os.environ['LANG'] = 'C.UTF-8'
#        result = runner.invoke(run, ['examples/workflow_with_file_output.yaml',
#                                     '--cyverse_token', cyverse_token])
#        assert result.exit_code == 0
#
#        # check file was written
#        assert isfile(from_path)
#        check_hello(from_path, 'world')
#        os.remove(from_path)
#
#        # check file was pushed to iRODS
#        session.data_objects.get(join(collection, 'output.txt'), from_path)
#        check_hello(from_path, 'world')
#        os.remove(from_path)
#    finally:
#        clear_dir(testdir)
#        session.collections.remove(collection, force=True)


#def test_workflow_with_directory_output(cyverse_token, workflow_with_directory_output):
#    f_path = join(testdir, workflow_with_directory_output.output['from_path'])
#    collection = join(from_path, "testCollection")
#    output_1_path = join(f_path, 't1.txt')
#    output_2_path = join(f_path, 't2.txt')
#
#    try:
#        # prep iRODS collection
#        print(requests.post('https://de.cyverse.org/terrain/secured/filesystem/directory/create',
#                            json={'path': collection},
#                            headers={'Authorization': 'Bearer ' + cyverse_token}).json())
#
#        # execute the workflow
#        os.environ['LC_ALL'] = 'C.UTF-8'
#        os.environ['LANG'] = 'C.UTF-8'
#        result = runner.invoke(run, ['examples/workflow_with_directory_output.yaml',
#                                     '--cyverse_token', cyverse_token])
#        assert result.exit_code == 0
#
#        # check files were written
#        assert isfile(output_1_path)
#        assert isfile(output_2_path)
#        check_hello(output_1_path, 'world')
#        check_hello(output_2_path, 'world')
#        os.remove(output_1_path)
#        os.remove(output_2_path)
#
#        # check files were pushed to CyVerse
#        files = requests.get(f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?path={collection}&limit=1000", headers={'Authorization': 'Bearer ' + cyverse_token}).json()['files']
#        assert join(collection, 't1.txt') in [file['path'] for file in files]
#        assert join(collection, 't2.txt') in [file['path'] for file in files]
#        assert isfile(output_1_path)
#        assert isfile(output_2_path)
#        check_hello(output_1_path, 'world')
#        check_hello(output_2_path, 'world')
#        os.remove(output_1_path)
#        os.remove(output_2_path)
#    finally:
#        #clear_dir(testdir)
#        print(requests.post('https://de.cyverse.org/terrain/secured/filesystem/delete',
#                            json={'paths': [
#                                collection
#                            ]},
#                            headers={'Authorization': 'Bearer ' + cyverse_token}).json())
