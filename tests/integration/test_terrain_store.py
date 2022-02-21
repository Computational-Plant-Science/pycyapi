import uuid
import os
from tempfile import TemporaryDirectory
from os.path import join, isfile

import pytest
from plantit_cli.store import terrain_store

from plantit_cli.tests.integration.test_utils import delete_collection, upload_file, create_collection, list_files
from plantit_cli.tests.utils import TerrainToken

message = "Message!"
token = TerrainToken.get()


def test_directory_exists(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))

    try:
        # prep collection
        create_collection(remote_path, token)

        # test remote directories exist
        assert terrain_store.dir_exists(remote_path, token)
        assert not terrain_store.dir_exists(join(remote_base_path, "notCollection"), token)
    finally:
        delete_collection(remote_path, token)


def test_file_exists(remote_base_path):
    with TemporaryDirectory() as testdir:
        file1_name = 'f1.txt'
        file2_name = 'f2.txt'
        file1_path = join(testdir, file1_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(remote_path, token)

            # create files
            with open(file1_path, "w") as file1:
                file1.write('Hello, 1!')

            # upload files
            upload_file(file1_path, remote_path, token)

            # test remote files exist
            assert terrain_store.file_exists(join(remote_path, file1_name), token)
            assert not terrain_store.file_exists(join(remote_path, file2_name), token)
        finally:
            delete_collection(remote_path, token)


def test_list_directory(remote_base_path):
    with TemporaryDirectory() as testdir:
        file1_name = 'f1.txt'
        file2_name = 'f2.txt'
        file1_path = join(testdir, file1_name)
        file2_path = join(testdir, file2_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(remote_path, token)

            # create files
            with open(file1_path, "w") as file1, open(file2_path, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # upload files
            upload_file(file1_path, remote_path, token)
            upload_file(file2_path, remote_path, token)

            # list files
            files = terrain_store.list_dir(remote_path, token)

            # check files
            assert join(remote_path, file1_name) in files
            assert join(remote_path, file2_name) in files
        finally:
            delete_collection(remote_path, token)


def test_list_directory_no_retries_when_path_does_not_exist(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))
    with pytest.raises(ValueError):
        terrain_store.list_dir(remote_path, token)


def test_list_directory_retries_when_token_invalid(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))
    with pytest.raises(ValueError):
        terrain_store.list_dir(remote_path, token)


def test_download_file(remote_base_path):
    with TemporaryDirectory() as testdir:
        file_name = 'f1.txt'
        file_path = join(testdir, file_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(remote_path, token)

            # create files
            with open(file_path, "w") as file:
                file.write('Hello, 1!')

            # upload files
            upload_file(file_path, remote_path, token)

            # download file
            terrain_store.pull_file(join(remote_path, file_name), testdir, token)

            # check download
            assert isfile(file_path)
        finally:
            delete_collection(remote_path, token)


def test_download_directory(remote_base_path):
    with TemporaryDirectory() as testdir:
        file1_name = 'f1.txt'
        file2_name = 'f2.txt'
        file1_path = join(testdir, file1_name)
        file2_path = join(testdir, file2_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(remote_path, token)

            # create files
            with open(file1_path, "w") as file1, open(file2_path, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # upload files
            upload_file(file1_path, remote_path, token)
            upload_file(file2_path, remote_path, token)

            # remove files locally
            os.remove(file1_path)
            os.remove(file2_path)

            # download files
            terrain_store.pull_dir(remote_path, testdir, token, ['.txt'])

            # check downloads
            assert isfile(file1_path)
            assert isfile(file2_path)
        finally:
            delete_collection(remote_path, token)


def test_upload_file(remote_base_path):
    with TemporaryDirectory() as testdir:
        file_name = 'f1.txt'
        file_path = join(testdir, file_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(remote_path, token)

            # create files
            with open(file_path, "w") as file:
                file.write('Hello, 1!')

            # upload file
            terrain_store.push_file(file_path, remote_path, token)

            # check download
            files = list_files(remote_path, token)
            assert file_name in files
        finally:
            delete_collection(remote_path, token)


def test_upload_directory(remote_base_path):
    with TemporaryDirectory() as testdir:
        file_name1 = 'f1.txt'
        file_name2 = 'f2.txt'
        file_path1 = join(testdir, file_name1)
        file_path2 = join(testdir, file_name2)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(remote_path, token)

            # create files
            with open(file_path1, "w") as file1, open(file_path2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # upload directory
            terrain_store.push_dir(testdir, remote_path, token)

            # check download
            files = list_files(remote_path, token)
            assert file_name1 in files
            assert file_name2 in files
        finally:
            delete_collection(remote_path, token)
