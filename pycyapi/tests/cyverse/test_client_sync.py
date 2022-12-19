import uuid
from os import environ, remove
from os.path import isfile, join
from tempfile import TemporaryDirectory

import pytest
from flaky import flaky
from requests import HTTPError

from pycyapi.cyverse.auth import CyverseAccessToken
from pycyapi.cyverse.clients import CyverseClient
from pycyapi.tests.conftest import (
    create_collection,
    delete_collection,
    get_metadata,
    list_files,
    set_metadata,
    stat_file,
    upload_file,
)

message = "Message"
token = CyverseAccessToken.get()
client = CyverseClient(token)


@flaky(max_runs=3)
def test_get_user_info():
    username = environ.get("CYVERSE_USERNAME")
    user_info = client.user_info(username)
    assert user_info["id"] == username


@flaky(max_runs=3)
def test_throws_error_when_token_is_invalid():
    with pytest.raises(HTTPError) as e:
        client = CyverseClient("not a valid token")
        client.exists(
            "/iplant/home/shared/iplantcollaborative/testing_tools/cowsay"
        )
        assert "401" in str(e)


@flaky(max_runs=3)
def test_path_exists_when_doesnt():
    exists = client.exists(
        "/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsaid.txt"
    )
    assert not exists


@flaky(max_runs=3)
def test_path_exists_when_is_a_file():
    exists = client.exists(
        "/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsay.txt"
    )
    assert exists


@flaky(max_runs=3)
def test_path_exists_when_is_a_directory():
    # with trailing slash
    exists = client.exists(
        "/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/"
    )
    assert exists

    # without trailing slash
    exists = client.exists(
        "/iplant/home/shared/iplantcollaborative/testing_tools/cowsay"
    )
    assert exists


@flaky(max_runs=3)
@pytest.mark.slow
def test_dir_exists_when_is_a_directory(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))

    try:
        # prep collection
        create_collection(token, remote_path)

        # test remote directories exist
        assert client.dir_exists(remote_path)
        assert not client.dir_exists(join(remote_base_path, "notCollection"))
    finally:
        delete_collection(token, remote_path)


@flaky(max_runs=3)
@pytest.mark.slow
def test_dir_exists_when_is_a_file(remote_base_path):
    with TemporaryDirectory() as testdir:
        file1_name = "f1.txt"
        file1_path = join(testdir, file1_name)
        remote_dir_path = join(remote_base_path, str(uuid.uuid4()))
        remote_file_path = join(remote_dir_path, file1_name)

        try:
            # prep collection
            create_collection(token, remote_dir_path)

            # create files
            with open(file1_path, "w") as file1:
                file1.write("Hello, 1!")

            # upload files
            upload_file(token, file1_path, remote_dir_path)

            # check if path exists
            assert client.dir_exists(remote_base_path)
            assert client.dir_exists(remote_dir_path)
            assert not client.dir_exists(remote_file_path)
        finally:
            delete_collection(token, remote_dir_path)


@flaky(max_runs=3)
@pytest.mark.slow
def test_file_exists_when_is_a_file(remote_base_path):
    with TemporaryDirectory() as testdir:
        file1_name = "f1.txt"
        file2_name = "f2.txt"
        file1_path = join(testdir, file1_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(token, remote_path)

            # create files
            with open(file1_path, "w") as file1:
                file1.write("Hello, 1!")

            # upload files
            upload_file(token, file1_path, remote_path)

            # test remote files exist
            assert client.file_exists(join(remote_path, file1_name))
            assert not client.file_exists(join(remote_path, file2_name))
        finally:
            delete_collection(token, remote_path)


@flaky(max_runs=3)
@pytest.mark.slow
def test_file_exists_when_is_a_directory(remote_base_path):
    with TemporaryDirectory() as testdir:
        file1_name = "f1.txt"
        file2_name = "f2.txt"
        file1_path = join(testdir, file1_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(token, remote_path)

            # create files
            with open(file1_path, "w") as file1:
                file1.write("Hello, 1!")

            # upload files
            upload_file(token, file1_path, remote_path)

            # test if path exists
            assert client.file_exists(join(remote_path, file1_name))
            assert not client.file_exists(remote_base_path)
        finally:
            delete_collection(token, remote_path)


@flaky(max_runs=3)
@pytest.mark.slow
def test_list(remote_base_path):
    with TemporaryDirectory() as testdir:
        file1_name = "f1.txt"
        file2_name = "f2.txt"
        file1_path = join(testdir, file1_name)
        file2_path = join(testdir, file2_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(token, remote_path)

            # create files
            with open(file1_path, "w") as file1, open(
                file2_path, "w"
            ) as file2:
                file1.write("Hello, 1!")
                file2.write("Hello, 2!")

            # upload files
            upload_file(token, file1_path, remote_path)
            upload_file(token, file2_path, remote_path)

            # list files
            paths = [
                file["path"] for file in client.list(remote_path)["files"]
            ]

            # check files
            assert join(remote_path, file1_name) in paths
            assert join(remote_path, file2_name) in paths
        finally:
            delete_collection(token, remote_path)


@flaky(max_runs=3)
def test_list_no_retries_when_path_does_not_exist(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))
    with pytest.raises(ValueError):
        client.list(remote_path)


@flaky(max_runs=3)
def test_list_retries_when_token_invalid(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))
    with pytest.raises(ValueError):
        client.list(remote_path)


@flaky(max_runs=3)
@pytest.mark.slow
def test_mkdir(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))

    try:
        client.mkdir(remote_path)

        # check dir exists
        assert client.dir_exists(remote_path)
    finally:
        delete_collection(token, remote_path)


@flaky(max_runs=3)
@pytest.mark.skip(reason="todo")
def test_share(remote_base_path):
    # TODO: how to test this? might need 2 CyVerse accounts
    pass


@flaky(max_runs=3)
@pytest.mark.skip(reason="todo")
def test_unshare(remote_base_path):
    # TODO: how to test this? might need 2 CyVerse accounts
    pass


@flaky(max_runs=3)
@pytest.mark.slow
def test_download_file(remote_base_path):
    with TemporaryDirectory() as testdir:
        file_name = "f1.txt"
        file_path = join(testdir, file_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(token, remote_path)

            # create files
            with open(file_path, "w") as file:
                file.write("Hello, 1!")

            # upload files
            upload_file(token, file_path, remote_path)

            # download file
            client.download(join(remote_path, file_name), testdir)

            # check download
            assert isfile(file_path)
        finally:
            delete_collection(token, remote_path)


@flaky(max_runs=3)
@pytest.mark.slow
def test_download_directory(remote_base_path):
    with TemporaryDirectory() as testdir:
        file1_name = "f1.txt"
        file2_name = "f2.txt"
        file1_path = join(testdir, file1_name)
        file2_path = join(testdir, file2_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(token, remote_path)

            # create files
            with open(file1_path, "w") as file1, open(
                file2_path, "w"
            ) as file2:
                file1.write("Hello, 1!")
                file2.write("Hello, 2!")

            # upload files
            upload_file(token, file1_path, remote_path)
            upload_file(token, file2_path, remote_path)

            # remove files locally
            remove(file1_path)
            remove(file2_path)

            # download files
            client.download_directory(remote_path, testdir, [".txt"])

            # check downloads
            assert isfile(file1_path)
            assert isfile(file2_path)
        finally:
            delete_collection(token, remote_path)


@flaky(max_runs=3)
@pytest.mark.slow
def test_upload_file(remote_base_path):
    with TemporaryDirectory() as testdir:
        file_name = "f1.txt"
        file_path = join(testdir, file_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(token, remote_path)

            # create files
            with open(file_path, "w") as file:
                file.write("Hello, 1!")

            # upload file
            client.upload(file_path, remote_path)

            # check upload
            paths = list_files(token, remote_path)
            assert file_name in paths
        finally:
            delete_collection(token, remote_path)


@flaky(max_runs=3)
@pytest.mark.slow
def test_upload_directory(remote_base_path):
    with TemporaryDirectory() as testdir:
        file_name1 = "f1.txt"
        file_name2 = "f2.txt"
        file_path1 = join(testdir, file_name1)
        file_path2 = join(testdir, file_name2)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(token, remote_path)

            # create files
            with open(file_path1, "w") as file1, open(
                file_path2, "w"
            ) as file2:
                file1.write("Hello, 1!")
                file2.write("Hello, 2!")

            # upload directory
            client.upload_directory(testdir, remote_path)

            # check upload
            paths = list_files(token, remote_path)
            assert file_name1 in paths
            assert file_name2 in paths
        finally:
            delete_collection(token, remote_path)


@flaky(max_runs=3)
@pytest.mark.slow
def test_set_metadata(remote_base_path):
    with TemporaryDirectory() as testdir:
        file_name = "f1.txt"
        file_path = join(testdir, file_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(token, remote_path)

            # create files
            with open(file_path, "w") as file:
                file.write("Hello, 1!")

            # upload file
            upload_file(token, file_path, remote_path)

            # get file info and checksum
            info = stat_file(token, remote_path)
            id = info["id"]

            # set file metadata
            client.set_metadata(id, ["k1=v1", "k2=v2"], ["k3=v3"])

            # check metadata was set
            metadata = get_metadata(token, id)
            assert len(metadata["avus"]) == 2
            assert len(metadata["irods-avus"]) == 1
            assert any(
                d
                for d in metadata["avus"]
                if d["attr"] == "k1" and d["value"] == "v1"
            )
            assert any(
                d
                for d in metadata["avus"]
                if d["attr"] == "k2" and d["value"] == "v2"
            )
            assert any(
                d
                for d in metadata["irods-avus"]
                if d["attr"] == "k3" and d["value"] == "v3"
            )
        finally:
            delete_collection(token, remote_path)


@flaky(max_runs=3)
@pytest.mark.slow
def test_get_metadata(remote_base_path):
    with TemporaryDirectory() as testdir:
        file_name = "f1.txt"
        file_path = join(testdir, file_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))

        try:
            # prep collection
            create_collection(token, remote_path)

            # create files
            with open(file_path, "w") as file:
                file.write("Hello, 1!")

            # upload file
            upload_file(token, file_path, remote_path)

            # get file info and checksum
            info = stat_file(token, remote_path)
            id = info["id"]

            # set metadata
            set_metadata(token, id, ["k1=v1", "k2=v2"])

            # get metadata and check it
            metadata = client.get_metadata(id)
            assert len(metadata) == 2
            assert any(
                d for d in metadata if d["attr"] == "k1" and d["value"] == "v1"
            )
            assert any(
                d for d in metadata if d["attr"] == "k2" and d["value"] == "v2"
            )
        finally:
            delete_collection(token, remote_path)
