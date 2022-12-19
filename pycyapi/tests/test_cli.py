import uuid
from os import environ, remove
from os.path import basename, isfile, join
from pprint import pprint
from tempfile import TemporaryDirectory
from time import sleep

import pytest
from click.testing import CliRunner
from flaky import flaky

import pycyapi.cli
from pycyapi.cyverse.auth import CyverseAccessToken
from pycyapi.tests.conftest import (
    create_collection,
    delete_collection,
    get_metadata,
    list_directories,
    list_files,
    set_metadata,
    stat_file,
    upload_file,
)


@flaky(max_runs=3)
@pytest.mark.skip(reason="todo")
def test_scripts_slurm_no_source_no_sink():
    pass


@flaky(max_runs=3)
@pytest.mark.skip(reason="todo")
def test_scripts_slurm_with_source():
    pass


@flaky(max_runs=3)
@pytest.mark.skip(reason="todo")
def test_scripts_slurm_with_sink():
    pass


@flaky(max_runs=3)
@pytest.mark.skip(reason="todo")
def test_scripts_slurm_with_source_and_sink():
    pass


username = environ.get("CYVERSE_USERNAME")
password = environ.get("CYVERSE_PASSWORD")
token = CyverseAccessToken.get()


@flaky(max_runs=3)
@pytest.mark.slow
def test_cas_token():
    runner = CliRunner()
    result = runner.invoke(
        pycyapi.cli.token, ["--username", username, "--password", password]
    )
    tkn = result.output.strip()

    assert tkn != ""
    # make sure we can make a request with the token
    pprint(list_directories(token=tkn, path=f"iplant/home/{username}/"))


@flaky(max_runs=3)
def test_create_directory_when_token_is_invalid(remote_base_path):
    pass


@flaky(max_runs=3)
@pytest.mark.slow
def test_create_directory(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))

    try:
        runner = CliRunner()
        runner.invoke(pycyapi.cli.create, ["--token", token, remote_path])

        directories = list_directories(
            token=token, path=f"iplant/home/{username}/"
        )
        assert basename(remote_path) in directories
    finally:
        delete_collection(token, remote_path)


@flaky(max_runs=3)
def test_exists_when_doesnt_exist(remote_base_path):
    runner = CliRunner()
    result = runner.invoke(
        pycyapi.cli.exists,
        [
            "--token",
            token,
            "/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsaid.txt",
        ],
    )
    assert "False" in result.output


@flaky(max_runs=3)
def test_exists_when_is_a_file(remote_base_path):
    runner = CliRunner()
    result = runner.invoke(
        pycyapi.cli.exists,
        [
            "--token",
            token,
            "/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsay.txt",
        ],
    )
    assert "True" in result.output


@flaky(max_runs=3)
def test_exists_when_is_a_directory(remote_base_path):
    runner = CliRunner()

    # with trailing slash
    result = runner.invoke(
        pycyapi.cli.exists,
        [
            "--token",
            token,
            "/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/",
        ],
    )
    assert "True" in result.output

    # without trailing slash
    result = runner.invoke(
        pycyapi.cli.exists,
        [
            "--token",
            token,
            "/iplant/home/shared/iplantcollaborative/testing_tools/cowsay",
        ],
    )
    assert "True" in result.output


@flaky(max_runs=3)
@pytest.mark.skip(reason="todo")
def test_exists_with_type_file_when_is_a_file(remote_base_path):
    pass


@flaky(max_runs=3)
@pytest.mark.skip(reason="todo")
def test_exists_with_type_file_when_is_a_directory(remote_base_path):
    pass


@flaky(max_runs=3)
@pytest.mark.skip(reason="todo")
def test_exists_with_type_dir_when_is_a_file(remote_base_path):
    pass


@flaky(max_runs=3)
@pytest.mark.skip(reason="todo")
def test_exists_with_type_dir_when_is_a_directory(remote_base_path):
    pass


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
            runner = CliRunner()
            result = runner.invoke(
                pycyapi.cli.list, ["--token", token, remote_path]
            )

            # check files
            assert join(remote_path, file1_name) in result.output
            assert join(remote_path, file2_name) in result.output
        finally:
            delete_collection(token, remote_path)


@flaky(max_runs=3)
@pytest.mark.skip(reason="todo")
def test_share_directory(remote_base_path):
    # TODO: how to test this? might need 2 CyVerse accounts
    pass


@flaky(max_runs=3)
@pytest.mark.skip(reason="todo")
def test_unshare_directory(remote_base_path):
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
            runner = CliRunner()
            runner.invoke(
                pycyapi.cli.pull,
                [
                    "--token",
                    token,
                    "--local_path",
                    testdir,
                    join(remote_path, file_name),
                ],
            )
        except ValueError as e:
            if "I/O operation on closed file" not in str(e):
                raise e
        finally:
            # check download
            assert isfile(file_path)

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
            runner = CliRunner()
            result = runner.invoke(
                pycyapi.cli.pull,
                ["--token", token, "--local_path", testdir, remote_path],
            )
        except ValueError as e:
            if "I/O operation on closed file" not in str(e):
                raise e
        finally:
            # check downloads
            assert isfile(file1_path)
            assert isfile(file2_path)

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
            runner = CliRunner()
            runner.invoke(
                pycyapi.cli.push,
                ["--token", token, "--local_path", file_path, remote_path],
            )
        except ValueError as e:
            if "I/O operation on closed file" not in str(e):
                raise e
        finally:
            # check upload
            paths = list_files(token, remote_path)
            assert file_name in paths

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
            runner = CliRunner()
            result = runner.invoke(
                pycyapi.cli.push,
                [
                    "--token",
                    token,
                    "--local_path",
                    testdir,
                    remote_path,
                    "-ip",
                    ".txt",
                ],
            )
        except ValueError as e:
            if "I/O operation on closed file" not in str(e):
                raise e
        finally:
            # check upload
            paths = list_files(token, remote_path)
            assert file_name1 in paths
            assert file_name2 in paths

            delete_collection(token, remote_path)


@flaky(max_runs=3)
@pytest.mark.slow
def test_tag(remote_base_path):
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
            runner = CliRunner()
            runner.invoke(
                pycyapi.cli.tag,
                [
                    "--token",
                    token,
                    "-a",
                    "k1=v1",
                    "-a",
                    "k2=v2",
                    "-ia",
                    "k3=v3",
                    id,
                ],
            )
        except ValueError as e:
            if "I/O operation on closed file" not in str(e):
                raise e
        finally:
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

            delete_collection(token, remote_path)


@flaky(max_runs=3)
@pytest.mark.skip(
    "todo debug UnboundLocalError: local variable 'result' referenced before assignment"
)
@pytest.mark.slow
def test_tags(remote_base_path):
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

            # get file metadata
            runner = CliRunner()
            result = runner.invoke(pycyapi.cli.tags, ["--token", token, id])
            print(result)
        except ValueError as e:
            if "I/O operation on closed file" not in str(e):
                raise e
        finally:
            sleep(1)
            # check metadata was set
            metadata = result.output.splitlines()
            assert len(metadata) == 2
            assert any(a for a in metadata if "k1=v1" in a)
            assert any(a for a in metadata if "k2=v2" in a)

            delete_collection(token, remote_path)
