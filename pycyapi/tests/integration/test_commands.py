import uuid
from os import remove, environ
from os.path import join
from tempfile import TemporaryDirectory

from pycyapi import commands as commands
from pycyapi.tests.utils import check_hello
from pycyapi.auth import AccessToken
import pycyapi.tests.integration.utils as testutils

message = "Message"
token = AccessToken.get()


def test_cas_token(remote_base_path):
    username = environ.get('CYVERSE_USERNAME', None)
    password = environ.get('CYVERSE_PASSWORD', None)

    tkn = commands.cas_token(username, password)

    # make sure we can use the token successfully
    remote_path = join(remote_base_path, str(uuid.uuid4()))
    try:
        testutils.create_collection(tkn, remote_path)
    finally:
        testutils.delete_collection(tkn, remote_path)


def test_refresh_tokens(remote_base_path):
    username = environ.get('CYVERSE_USERNAME', None)
    password = environ.get('CYVERSE_PASSWORD', None)

    tkn = commands.cas_token(username, password)


def test_pull(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as test_dir:
        local_path_1 = join(test_dir, file_name_1)
        local_path_2 = join(test_dir, file_name_2)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            testutils.create_collection(token, remote_path)

            # prep files
            with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # push files to remote directory
            testutils.upload_file(token, local_path_1, remote_path)
            testutils.upload_file(token, local_path_2, remote_path)

            # remove local files
            remove(local_path_1)
            remove(local_path_2)

            # pull directory
            commands.download(remote_path, test_dir, token=token)

            # check files were pulled
            downloaded_path_1 = join(test_dir, file_name_1)
            downloaded_path_2 = join(test_dir, file_name_2)
            check_hello(downloaded_path_1, 1)
            check_hello(downloaded_path_2, 2)
            remove(downloaded_path_1)
            remove(downloaded_path_2)
        finally:
            testutils.delete_collection(token, remote_path)


def test_push(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as test_dir:
        local_path_1 = join(test_dir, file_name_1)
        local_path_2 = join(test_dir, file_name_2)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            testutils.create_collection(token, remote_path)

            # prep files
            with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # push directory
            commands.upload(test_dir, remote_path, token=token)

            # check files were pushed to store
            paths = testutils.list_files(token, remote_path)
            assert len(paths) == 2
            assert file_name_1 in paths
            assert file_name_2 in paths
        finally:
            testutils.delete_collection(token, remote_path)
