import uuid

from os import remove
from os.path import join, isfile
from tempfile import TemporaryDirectory
import pytest
from requests import HTTPError

from pycyde.commands import path_exists, dir_exists, file_exists, list_dir, pull_file, pull_dir, push, pull, push_dir, push_file
from pycyde.tests.integration.test_utils import delete_collection, upload_file, create_collection, list_files
from pycyde.tests.utils import check_hello
from pycyde.tokens import TerrainToken

message = "Message"
token = TerrainToken.get()


def test_path_exists_when_doesnt_exist_is_false():
    exists = path_exists('/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsaid.txt', token)
    assert not exists


def test_path_exists_when_is_a_file_is_true():
    exists = path_exists('/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsay.txt', token)
    assert exists


def test_path_exists_when_is_a_directory_is_true():
    exists = path_exists('/iplant/home/shared/iplantcollaborative/testing_tools/cowsay', token)
    assert exists


def test_path_exists_throws_error_when_terrain_token_is_invalid():
    with pytest.raises(HTTPError) as e:
        path_exists('/iplant/home/shared/iplantcollaborative/testing_tools/cowsay', 'not a token')
        assert '401' in str(e)


def test_dir_exists(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))

    try:
        # prep collection
        create_collection(remote_path, token)

        # test remote directories exist
        assert dir_exists(remote_path, token)
        assert not dir_exists(join(remote_base_path, "notCollection"), token)
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
            assert file_exists(join(remote_path, file1_name), token)
            assert not file_exists(join(remote_path, file2_name), token)
        finally:
            delete_collection(remote_path, token)


def test_list_dir(remote_base_path):
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
            files = list_dir(remote_path, token)

            # check files
            assert join(remote_path, file1_name) in files
            assert join(remote_path, file2_name) in files
        finally:
            delete_collection(remote_path, token)


def test_list_dir_no_retries_when_path_does_not_exist(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))
    with pytest.raises(ValueError):
        list_dir(remote_path, token)


def test_list_dir_retries_when_token_invalid(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))
    with pytest.raises(ValueError):
        list_dir(remote_path, token)


def test_pull_file(remote_base_path):
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
            pull_file(join(remote_path, file_name), testdir, token)

            # check download
            assert isfile(file_path)
        finally:
            delete_collection(remote_path, token)


def test_pull_directory(remote_base_path):
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
            remove(file1_path)
            remove(file2_path)

            # download files
            pull_dir(remote_path, testdir, token, ['.txt'])

            # check downloads
            assert isfile(file1_path)
            assert isfile(file2_path)
        finally:
            delete_collection(remote_path, token)


def test_push_file(remote_base_path):
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
            push_file(file_path, remote_path, token)

            # check download
            files = list_files(remote_path, token)
            assert file_name in files
        finally:
            delete_collection(remote_path, token)


def test_push_directory(remote_base_path):
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
            push_dir(testdir, remote_path, token)

            # check download
            files = list_files(remote_path, token)
            assert file_name1 in files
            assert file_name2 in files
        finally:
            delete_collection(remote_path, token)


def test_pull(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as test_dir:
        local_path_1 = join(test_dir, file_name_1)
        local_path_2 = join(test_dir, file_name_2)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(remote_path, token)

            # prep files
            with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # push files to remote directory
            upload_file(local_path_1, remote_path, token)
            upload_file(local_path_2, remote_path, token)

            # remove local files
            remove(local_path_1)
            remove(local_path_2)

            # pull directory
            pull(token, remote_path, test_dir)

            # check files were pulled
            downloaded_path_1 = join(test_dir, file_name_1)
            downloaded_path_2 = join(test_dir, file_name_2)
            check_hello(downloaded_path_1, 1)
            check_hello(downloaded_path_2, 2)
            remove(downloaded_path_1)
            remove(downloaded_path_2)
        finally:
            delete_collection(remote_path, token)


def test_push(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as test_dir:
        local_path_1 = join(test_dir, file_name_1)
        local_path_2 = join(test_dir, file_name_2)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(remote_path, token)

            # prep files
            with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # push directory
            push(token, test_dir, remote_path)

            # check files were pushed to store
            files = list_files(remote_path, token)
            assert len(files) == 2
            assert file_name_1 in files
            assert file_name_2 in files
        finally:
            delete_collection(remote_path, token)