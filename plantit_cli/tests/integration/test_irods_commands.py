import uuid
import pytest
from os import remove
from os.path import join
from tempfile import TemporaryDirectory

from plantit_cli.store import irods_commands
from plantit_cli.tests.integration.test_utils import delete_collection, upload_file, create_collection, list_files
from plantit_cli.tests.utils import check_hello, TerrainToken, TerrainTicket

message = "Message!"
token = TerrainToken.get()


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
            ticket = TerrainTicket.get([remote_path])
            irods_commands.pull(remote_path, test_dir, ticket)

            # check files were pulled
            downloaded_path_1 = join(test_dir, file_name_1)
            downloaded_path_2 = join(test_dir, file_name_2)
            check_hello(downloaded_path_1, 1)
            check_hello(downloaded_path_2, 2)
            remove(downloaded_path_1)
            remove(downloaded_path_2)
        finally:
            delete_collection(remote_path, token)


@pytest.mark.skip(reason="pending python-irodsclient issues")
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
            ticket = TerrainTicket.get([remote_path], mode='write')
            irods_commands.push(test_dir, remote_path, ticket)

            # check files were pushed to store
            files = list_files(remote_path, token)
            assert len(files) == 2
            assert file_name_1 in files
            assert file_name_2 in files
        finally:
            delete_collection(remote_path, token)










