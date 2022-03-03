import uuid
from os import remove
from os.path import join, isfile
from tempfile import TemporaryDirectory

import pytest
from requests import HTTPError

from pycyapi.clients import TerrainClient
import pycyapi.tests.integration.utils as testutils
from pycyapi.auth import AccessToken

message = "Message"
token = AccessToken.get()
client = TerrainClient(token)


def test_path_exists_when_doesnt_exist_is_false():
    exists = client.exists('/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsaid.txt')
    assert not exists


def test_path_exists_when_is_a_file_is_true():
    exists = client.exists('/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsay.txt')
    assert exists


def test_path_exists_when_is_a_directory_is_true():
    exists = client.exists('/iplant/home/shared/iplantcollaborative/testing_tools/cowsay')
    assert exists


def test_path_exists_throws_error_when_terrain_token_is_invalid():
    with pytest.raises(HTTPError) as e:
        client = TerrainClient('not a valid token')
        client.exists('/iplant/home/shared/iplantcollaborative/testing_tools/cowsay')
        assert '401' in str(e)


def test_dir_exists(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))

    try:
        # prep collection
        testutils.create_collection(token, remote_path)

        # test remote directories exist
        assert client.dir_exists(remote_path)
        assert not client.dir_exists(join(remote_base_path, "notCollection"))
    finally:
        testutils.delete_collection(token, remote_path)


def test_file_exists(remote_base_path):
    with TemporaryDirectory() as testdir:
        file1_name = 'f1.txt'
        file2_name = 'f2.txt'
        file1_path = join(testdir, file1_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            testutils.create_collection(token, remote_path)

            # create files
            with open(file1_path, "w") as file1:
                file1.write('Hello, 1!')

            # upload files
            testutils.upload_file(token, file1_path, remote_path)

            # test remote files exist
            assert client.file_exists(join(remote_path, file1_name))
            assert not client.file_exists(join(remote_path, file2_name))
        finally:
            testutils.delete_collection(token, remote_path)


def test_list_files(remote_base_path):
    with TemporaryDirectory() as testdir:
        file1_name = 'f1.txt'
        file2_name = 'f2.txt'
        file1_path = join(testdir, file1_name)
        file2_path = join(testdir, file2_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            testutils.create_collection(token, remote_path)

            # create files
            with open(file1_path, "w") as file1, open(file2_path, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # upload files
            testutils.upload_file(token, file1_path, remote_path)
            testutils.upload_file(token, file2_path, remote_path)

            # list files
            paths = [file['path'] for file in client.paged_directory(remote_path)['files']]

            # check files
            assert join(remote_path, file1_name) in paths
            assert join(remote_path, file2_name) in paths
        finally:
            testutils.delete_collection(token, remote_path)


def test_list_dir_no_retries_when_path_does_not_exist(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))
    with pytest.raises(ValueError):
        client.paged_directory(remote_path)


def test_list_dir_retries_when_token_invalid(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))
    with pytest.raises(ValueError):
        client.paged_directory(remote_path)


def test_pull_file(remote_base_path):
    with TemporaryDirectory() as testdir:
        file_name = 'f1.txt'
        file_path = join(testdir, file_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            testutils.create_collection(token, remote_path)

            # create files
            with open(file_path, "w") as file:
                file.write('Hello, 1!')

            # upload files
            testutils.upload_file(token, file_path, remote_path)

            # download file
            client.download(join(remote_path, file_name), testdir)

            # check download
            assert isfile(file_path)
        finally:
            testutils.delete_collection(token, remote_path)


def test_pull_directory(remote_base_path):
    with TemporaryDirectory() as testdir:
        file1_name = 'f1.txt'
        file2_name = 'f2.txt'
        file1_path = join(testdir, file1_name)
        file2_path = join(testdir, file2_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            testutils.create_collection(token, remote_path)

            # create files
            with open(file1_path, "w") as file1, open(file2_path, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # upload files
            testutils.upload_file(token, file1_path, remote_path)
            testutils.upload_file(token, file2_path, remote_path)

            # remove files locally
            remove(file1_path)
            remove(file2_path)

            # download files
            client.download_directory(remote_path, testdir, ['.txt'])

            # check downloads
            assert isfile(file1_path)
            assert isfile(file2_path)
        finally:
            testutils.delete_collection(token, remote_path)


def test_push_file(remote_base_path):
    with TemporaryDirectory() as testdir:
        file_name = 'f1.txt'
        file_path = join(testdir, file_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            testutils.create_collection(token, remote_path)

            # create files
            with open(file_path, "w") as file:
                file.write('Hello, 1!')

            # upload file
            client.upload(file_path, remote_path)

            # check upload
            paths = testutils.list_files(token, remote_path)
            assert file_name in paths
        finally:
            testutils.delete_collection(token, remote_path)


def test_push_directory(remote_base_path):
    with TemporaryDirectory() as testdir:
        file_name1 = 'f1.txt'
        file_name2 = 'f2.txt'
        file_path1 = join(testdir, file_name1)
        file_path2 = join(testdir, file_name2)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            testutils.create_collection(token, remote_path)

            # create files
            with open(file_path1, "w") as file1, open(file_path2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # upload directory
            client.upload_directory(testdir, remote_path)

            # check upload
            paths = testutils.list_files(token, remote_path)
            assert file_name1 in paths
            assert file_name2 in paths
        finally:
            testutils.delete_collection(token, remote_path)
