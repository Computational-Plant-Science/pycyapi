import os
from os import environ
from os.path import join, isfile

from plantit_cli.store.terrain_store import TerrainStore
from plantit_cli.run import Run
from plantit_cli.tests.terrain_test_utils import delete_collection, upload_file, create_collection
from plantit_cli.tests.test_utils import clear_dir, get_token

message = "Message!"
testdir = environ.get('TEST_DIRECTORY')
token = get_token()


def _run(remote_base_path):
    return Run(
        identifier='workflow_with_directory_input',
        workdir=testdir,
        image="docker://alpine:latest",
        command='ls $INPUT | tee $INPUT.output',
        input={
            'kind': 'directory',
            'from': join(remote_base_path, "testCollection"),
        },
        cyverse_token=token)


def test_list_directory(remote_base_path):
    file1_name = 'f1.txt'
    file2_name = 'f2.txt'
    file1_path = join(testdir, file1_name)
    file2_path = join(testdir, file2_name)
    remote_path = join(remote_base_path, "testCollection")
    store = TerrainStore(token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # create files
        with open(file1_path, "w") as file1, open(file2_path, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')

        # upload files to CyVerse
        upload_file(file1_path, remote_path, token)
        upload_file(file2_path, remote_path, token)

        # list files
        files = store.list_directory(remote_path)

        # check listed files
        assert join(remote_path, file1_name) in files
        assert join(remote_path, file2_name) in files
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_download_file(remote_base_path):
    file_name = 'f1.txt'
    file_path = join(testdir, file_name)
    remote_path = join(remote_base_path, "testCollection")
    store = TerrainStore(token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # create files
        with open(file_path, "w") as file:
            file.write('Hello, 1!')

        # upload files to CyVerse
        upload_file(file_path, remote_path, token)

        # download file
        store.download_file(join(remote_path, file_name), testdir)

        # check file was downloaded
        assert isfile(file_path)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


def test_download_directory(remote_base_path):
    file1_name = 'f1.txt'
    file2_name = 'f2.txt'
    file1_path = join(testdir, file1_name)
    file2_path = join(testdir, file2_name)
    remote_path = join(remote_base_path, "testCollection")
    store = TerrainStore(token)

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # create files
        with open(file1_path, "w") as file1, open(file2_path, "w") as file2:
            file1.write('Hello, 1!')
            file2.write('Hello, 2!')

        # upload files to CyVerse
        upload_file(file1_path, remote_path, token)
        upload_file(file2_path, remote_path, token)

        # remove files
        os.remove(file1_path)
        os.remove(file2_path)

        # download files
        store.download_directory(remote_path, testdir, '.txt')

        # check files were downloaded
        assert isfile(file1_path)
        assert isfile(file2_path)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)
