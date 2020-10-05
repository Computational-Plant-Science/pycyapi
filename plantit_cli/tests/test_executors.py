import os
import tempfile
import time
from os.path import join, isfile

import pytest
import requests

from plantit_cli.tests.utils import clear_dir, check_hello

from_path = f"/iplant/home/{os.environ.get('CYVERSE_USERNAME')}"
message = "Message!"
testdir = '/test'
tempdir = tempfile.gettempdir()


def create_collection(cyverse_token, path):
    print(requests.post('https://de.cyverse.org/terrain/secured/filesystem/directory/create',
                        json={'path': path},
                        headers={'Authorization': 'Bearer ' + cyverse_token}).json())
    time.sleep(15)


def list_files(cyverse_token, path):
    time.sleep(15)
    response = requests.get(
        f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?path={path}&limit=1000",
        headers={'Authorization': 'Bearer ' + cyverse_token})
    content = response.json()
    print(content)
    return content['files']


def upload_file(cyverse_token, local_path, remote_path):
    print(requests.post(
        f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={remote_path}",
        files={'file': (local_path.split('/')[-1], '\n'.join(open(local_path, 'r').readlines()))},
        headers={'Authorization': 'Bearer ' + cyverse_token}).json())
    time.sleep(15)


def delete_collection(cyverse_token, path):
    print(requests.post('https://de.cyverse.org/terrain/secured/filesystem/delete',
                        json={'paths': [path]},
                        headers={'Authorization': 'Bearer ' + cyverse_token}).json())
    time.sleep(15)


def test_workflow_with_params(executor, workflow_with_params):
    try:
        # run the workflow
        executor.execute(workflow_with_params)

        # check local message file
        file = join(testdir, 'message.txt')
        assert isfile(file)
        with open(file) as file:
            lines = file.readlines()
            assert len(lines) == 1
            assert lines[0] == f"{message}\n"
    finally:
        clear_dir(testdir)


# def test_workflow_with_file_input(cyverse_token, executor, workflow_with_file_input):
#     local_file_1 = tempfile.NamedTemporaryFile()
#     local_file_2 = tempfile.NamedTemporaryFile()
#     local_file_1_name = local_file_1.name.split('/')[-1]
#     local_file_2_name = local_file_2.name.split('/')[-1]
#     remote_path = join(from_path, "testCollection")
#
#     try:
#         # prep CyVerse collection
#         create_collection(cyverse_token, remote_path)
#
#         # prep files
#         local_file_1.write(b'Hello, 1!')
#         local_file_1.seek(0)
#         local_file_2.write(b'Hello, 2!')
#         local_file_2.seek(0)
#         upload_file(cyverse_token, local_file_1.name, remote_path + '/')
#         upload_file(cyverse_token, local_file_2.name, remote_path + '/')
#         local_file_1.close()
#         local_file_2.close()
#
#         # run the workflow (expect 2 containers, 1 for each input file)
#         executor.execute(workflow_with_file_input)
#
#         # check input files were pulled from CyVerse
#         input_1 = join(testdir, 'input', local_file_1_name)
#         input_2 = join(testdir, 'input', local_file_2_name)
#         check_hello(input_1, 1)
#         check_hello(input_2, 2)
#         os.remove(input_1)
#         os.remove(input_2)
#
#         # check local output files were written
#         output_1 = f"{input_1}.output"
#         output_2 = f"{input_2}.output"
#         check_hello(output_1, 1)
#         check_hello(output_2, 2)
#         os.remove(output_1)
#         os.remove(output_2)
#     finally:
#         clear_dir(testdir)
#         delete_collection(cyverse_token, remote_path)
#
#
#
#
# def test_workflow_with_directory_input(session, executor, workflow_with_directory_input):
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
#        executor.execute(workflow_with_directory_input)
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


def test_workflow_with_file_output(cyverse_token, executor, workflow_with_file_output):
    local_output_path = join(testdir, workflow_with_file_output.output['from'])
    remote_path = join(from_path, "testCollection")

    try:
        # prep CyVerse collection
        create_collection(cyverse_token, remote_path)

        # run the workflow
        executor.execute(workflow_with_file_output)

        # check files were written locally
        assert isfile(local_output_path)
        check_hello(local_output_path, 'world')
        # os.remove(local_output_file)

        # check file was pushed to CyVerse
        files = list_files(cyverse_token, remote_path)
        assert join(remote_path, 'output.txt') in [file['path'] for file in files]
    finally:
        clear_dir(testdir)
        delete_collection(cyverse_token, remote_path)


def test_workflow_with_directory_output(cyverse_token, executor, workflow_with_directory_output):
    local_output_path = join(testdir, workflow_with_directory_output.output['from'])
    local_output_file_1 = join(local_output_path, 't1.txt')
    local_output_file_2 = join(local_output_path, 't2.txt')
    remote_path = join(from_path, "testCollection")

    try:
        # prep CyVerse collection
        create_collection(cyverse_token, remote_path)

        # execute the workflow
        executor.execute(workflow_with_directory_output)

        # check files were written locally
        assert isfile(local_output_file_1)
        assert isfile(local_output_file_2)
        check_hello(local_output_file_1, 'world')
        check_hello(local_output_file_2, 'world')
        os.remove(local_output_file_1)
        os.remove(local_output_file_2)

        # check files were pushed to CyVerse
        files = list_files(cyverse_token, remote_path)
        assert join(remote_path, 't1.txt') in [file['path'] for file in files]
        assert join(remote_path, 't2.txt') in [file['path'] for file in files]
    finally:
        clear_dir(testdir)
        delete_collection(cyverse_token, remote_path)
