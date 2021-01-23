from os import environ, remove
from os.path import join
from tempfile import TemporaryDirectory

import pytest

from plantit_cli import commands
from plantit_cli.store.local_store import LocalStore
from plantit_cli.tests.test_utils import clear_dir, get_token, check_hello

message = "Message"
test_dir = environ.get('TEST_DIRECTORY')
token = get_token()


@pytest.mark.skip(reason='TODO debug')
def test_pull_directory(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        local_path_1 = join(test_dir, file_name_1)
        local_path_2 = join(test_dir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")
        store = LocalStore(temp_dir)

        try:
            # prep files
            with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')
            store.push_file(local_path_1, remote_path)
            store.push_file(local_path_2, remote_path)

            # pull directory
            commands.pull(remote_path, test_dir, token)

            # check files were pulled
            downloaded_path_1 = join(test_dir, file_name_1)
            downloaded_path_2 = join(test_dir, file_name_2)
            check_hello(downloaded_path_1, 1)
            check_hello(downloaded_path_2, 2)
            remove(downloaded_path_1)
            remove(downloaded_path_2)
        finally:
            clear_dir(test_dir)


@pytest.mark.skip(reason='TODO debug')
def test_push(remote_base_path, file_name_1, file_name_2):
    with TemporaryDirectory() as temp_dir:
        local_path_1 = join(test_dir, file_name_1)
        local_path_2 = join(test_dir, file_name_2)
        remote_path = join(remote_base_path[1:], "testCollection")
        store = LocalStore(temp_dir)

        try:
            # prep files
            with open(local_path_1, "w") as file1, open(local_path_2, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # push directory
            commands.push(test_dir, remote_path, token)

            # check files were pushed
            uploaded_path_1 = join(remote_path, file_name_1)
            uploaded_path_2 = join(remote_path, file_name_2)

            # check files (included zipped) were pushed to store
            files = store.list_dir(remote_path)
            print(files)
            assert len(files) == 2
            assert store.file_exists(uploaded_path_1)
            assert store.file_exists(uploaded_path_2)
        finally:
            clear_dir(test_dir)
