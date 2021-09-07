import os
from os import environ
from os.path import join, isfile

import pytest
from plantit_cli.store import terrain_store

from plantit_cli.tests.integration.terrain_test_utils import delete_collection, upload_file, create_collection
from plantit_cli.tests.utils import clear_dir, Token

message = "Message!"
testdir = environ.get('TEST_DIRECTORY')
token = '' # Token.get()


@pytest.mark.skip(reason='debug')
def test_directory_exists(remote_base_path):
    remote_path = join(remote_base_path, "testCollection")

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # test directories exist
        assert terrain_store.dir_exists(remote_path, token)
        assert not terrain_store.dir_exists(join(remote_base_path, "notCollection"), token)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


@pytest.mark.skip(reason='debug')
def test_file_exists(remote_base_path):
    file1_name = 'f1.txt'
    file2_name = 'f2.txt'
    file1_path = join(testdir, file1_name)
    remote_path = join(remote_base_path, "testCollection")

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # create files
        with open(file1_path, "w") as file1:
            file1.write('Hello, 1!')

        # upload files to CyVerse
        upload_file(file1_path, remote_path, token)

        # test files exist
        assert terrain_store.file_exists(join(remote_path, file1_name), token)
        assert not terrain_store.file_exists(join(remote_path, file2_name), token)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


@pytest.mark.skip(reason='debug')
def test_list_directory(remote_base_path):
    file1_name = 'f1.txt'
    file2_name = 'f2.txt'
    file1_path = join(testdir, file1_name)
    file2_path = join(testdir, file2_name)
    remote_path = join(remote_base_path, "testCollection")

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
        files = terrain_store.list_dir(remote_path, token)

        # check listed files
        assert join(remote_path, file1_name) in files
        assert join(remote_path, file2_name) in files
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


@pytest.mark.skip(reason='debug')
def test_list_directory_no_retries_when_path_does_not_exist(remote_base_path):
    remote_path = join(remote_base_path, "badCollection")

    with pytest.raises(ValueError):
        terrain_store.list_dir(remote_path, token)


@pytest.mark.skip(reason='debug')
def test_list_directory_retries_when_token_invalid(remote_base_path):
    remote_path = join(remote_base_path, "testCollection")

    with pytest.raises(ValueError):
        terrain_store.list_dir(remote_path, token)


@pytest.mark.skip(reason='debug')
def test_download_file(remote_base_path):
    file_name = 'f1.txt'
    file_path = join(testdir, file_name)
    remote_path = join(remote_base_path, "testCollection")

    try:
        # prep CyVerse collection
        create_collection(remote_path, token)

        # create files
        with open(file_path, "w") as file:
            file.write('Hello, 1!')

        # upload files to CyVerse
        upload_file(file_path, remote_path, token)

        # download file
        terrain_store.pull_file(join(remote_path, file_name), testdir, token)

        # check file was downloaded
        assert isfile(file_path)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


@pytest.mark.skip(reason='debug')
def test_download_directory(remote_base_path):
    file1_name = 'f1.txt'
    file2_name = 'f2.txt'
    file1_path = join(testdir, file1_name)
    file2_path = join(testdir, file2_name)
    remote_path = join(remote_base_path, "testCollection")

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
        terrain_store.pull_dir(remote_path, testdir, ['.txt'], token)

        # check files were downloaded
        assert isfile(file1_path)
        assert isfile(file2_path)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)
