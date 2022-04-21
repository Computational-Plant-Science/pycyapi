import uuid
import unittest
from os import environ, remove, listdir
from os.path import join, basename, isfile
from tempfile import TemporaryDirectory

from click.testing import CliRunner
from pycyapi import cli
import pycyapi.tests.integration.utils as testutils
from pycyapi.auth import AccessToken

username = environ.get('CYVERSE_USERNAME')
password = environ.get('CYVERSE_PASSWORD')
token = AccessToken.get()


def test_cas_token():
    runner = CliRunner()
    result = runner.invoke(cli.token, ['--username', username, '--password', password])
    tkn = result.output.strip()

    assert tkn != ''
    # make sure we can make a request with the token
    testutils.list_files(token=tkn, path=f"iplant/home/{username}/")


def test_create_directory_when_token_is_invalid(remote_base_path):
    pass


def test_create_directory(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))

    try:
        runner = CliRunner()
        runner.invoke(cli.create, ['--token', token, remote_path])

        directories = testutils.list_directories(token=token, path=f"iplant/home/{username}/")
        assert basename(remote_path) in directories
    finally:
        testutils.delete_collection(token, remote_path)


def test_exists_when_doesnt_exist(remote_base_path):
    runner = CliRunner()
    result = runner.invoke(cli.exists, ['--token', token, '/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsaid.txt'])
    assert 'False' in result.output


def test_exists_when_is_a_file(remote_base_path):
    runner = CliRunner()
    result = runner.invoke(cli.exists, ['--token', token, '/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/cowsay.txt'])
    assert 'True' in result.output


def test_exists_when_is_a_directory(remote_base_path):
    runner = CliRunner()

    # with trailing slash
    result = runner.invoke(cli.exists, ['--token', token, '/iplant/home/shared/iplantcollaborative/testing_tools/cowsay/'])
    assert 'True' in result.output

    # without trailing slash
    result = runner.invoke(cli.exists, ['--token', token, '/iplant/home/shared/iplantcollaborative/testing_tools/cowsay'])
    assert 'True' in result.output


def test_exists_with_type_file_when_is_a_file(remote_base_path):
    pass


def test_exists_with_type_file_when_is_a_directory(remote_base_path):
    pass


def test_exists_with_type_dir_when_is_a_file(remote_base_path):
    pass


def test_exists_with_type_dir_when_is_a_directory(remote_base_path):
    pass


def test_list(remote_base_path):
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
            runner = CliRunner()
            result = runner.invoke(cli.list, ['--token', token, remote_path])

            # check files
            assert join(remote_path, file1_name) in result.output
            assert join(remote_path, file2_name) in result.output
        finally:
            testutils.delete_collection(token, remote_path)


def test_share_directory(remote_base_path):
    # TODO: how to test this? might need 2 CyVerse accounts
    pass


def test_unshare_directory(remote_base_path):
    # TODO: how to test this? might need 2 CyVerse accounts
    pass


def test_download_file(remote_base_path):
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
            runner = CliRunner()
            runner.invoke(cli.download, ['--token', token, '--local_path', testdir, join(remote_path, file_name)])

            # check download
            assert isfile(file_path)
        finally:
            testutils.delete_collection(token, remote_path)


def test_download_directory(remote_base_path):
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
            runner = CliRunner()
            result = runner.invoke(cli.download, ['--token', token, '--local_path', testdir, remote_path])

            # check downloads
            assert isfile(file1_path)
            assert isfile(file2_path)
        finally:
            testutils.delete_collection(token, remote_path)


def test_upload_file(remote_base_path):
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
            runner = CliRunner()
            runner.invoke(cli.upload, ['--token', token, '--local_path', file_path, remote_path])

            # check upload
            paths = testutils.list_files(token, remote_path)
            assert file_name in paths
        finally:
            testutils.delete_collection(token, remote_path)


def test_upload_directory(remote_base_path):
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
            runner = CliRunner()
            result = runner.invoke(cli.upload, ['--token', token, '--local_path', testdir, remote_path, '-ip', '.txt'])

            # check upload
            paths = testutils.list_files(token, remote_path)
            assert file_name1 in paths
            assert file_name2 in paths
        finally:
            testutils.delete_collection(token, remote_path)


def test_tag(remote_base_path):
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
            testutils.upload_file(token, file_path, remote_path)

            # get file info and checksum
            info = testutils.stat_file(token, remote_path)
            id = info['id']

            # set file metadata
            runner = CliRunner()
            runner.invoke(cli.tag, ['--token', token, '-a', 'k1=v1', '-a', 'k2=v2', id])

            # check metadata was set
            metadata = testutils.get_metadata(token, id)
            assert len(metadata) == 2
            assert any(d for d in metadata if d['attr'] == 'k1' and d['value'] == 'v1')
            assert any(d for d in metadata if d['attr'] == 'k2' and d['value'] == 'v2')
        finally:
            testutils.delete_collection(token, remote_path)


@unittest.skip("not sure why 'I/O operation on closed file' error occurs")
def test_tags(remote_base_path):
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
            testutils.upload_file(token, file_path, remote_path)

            # get file info and checksum
            info = testutils.stat_file(token, remote_path)
            id = info['id']

            # set metadata
            testutils.set_metadata(token, id, ['k1=v1', 'k2=v2'])

            # get file metadata
            runner = CliRunner()
            result = runner.invoke(cli.tags, ['--token', token, id])

            # check metadata was set
            metadata = result.output
            assert len(metadata) == 2
            assert any(a for a in metadata if 'k1=v1' in a)
            assert any(a for a in metadata if 'k2=v2' in a)
        finally:
            testutils.delete_collection(token, remote_path)
