import uuid
from os import remove
from os.path import join
from tempfile import TemporaryDirectory

from plantit_cli.store import terrain_commands
from plantit_cli.tests.utils import clear_dir, check_hello, TerrainToken
from plantit_cli.tests.integration.test_utils import delete_collection, upload_file, create_collection

message = "Message"
token = TerrainToken.get()


def test_terrain_pull(remote_base_path, file_name_1, file_name_2):
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

            # TODO push files to remote directory

            # pull directory
            terrain_commands.pull(remote_path, test_dir, token)

            # check files were pulled
            downloaded_path_1 = join(test_dir, file_name_1)
            downloaded_path_2 = join(test_dir, file_name_2)
            check_hello(downloaded_path_1, 1)
            check_hello(downloaded_path_2, 2)
            remove(downloaded_path_1)
            remove(downloaded_path_2)
        finally:
            delete_collection(remote_path, token)


def test_terrain_push(remote_base_path, file_name_1, file_name_2):
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

            # TODO push files to remote directory

            # push directory
            terrain_commands.push(test_dir, remote_path, token)

            # check files were pushed
            uploaded_path_1 = join(remote_path, file_name_1)
            uploaded_path_2 = join(remote_path, file_name_2)

            # TODO check files (included zipped) were pushed to store
            # files = store.list_dir(remote_path)
            # print(files)
            # assert len(files) == 2
            # assert store.file_exists(uploaded_path_1)
            # assert store.file_exists(uploaded_path_2)
        finally:
            delete_collection(remote_path, token)