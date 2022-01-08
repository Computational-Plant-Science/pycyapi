import uuid
import os
from os import environ
from os.path import join, isfile

import pytest
from irods.session import iRODSSession
from irods.ticket import Ticket

from plantit_cli.store import irods_store
from plantit_cli.tests.integration.test_utils import delete_collection, upload_file, create_collection
from plantit_cli.tests.utils import clear_dir, TerrainToken, TerrainTicket

message = "Message!"
testdir = environ.get('TEST_DIRECTORY')
token = TerrainToken.get()


# @pytest.mark.skip(reason='debug')
def test_directory_exists(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))

    try:
        # prep collection
        create_collection(remote_path, token)

        # create iRODS session
        session = iRODSSession(host='data.cyverse.org', port=1247, user='anonymous', password='', zone='iplant')
        ticket = TerrainTicket.get(remote_path)
        Ticket(session, ticket).supply()

        # test remote directories exist
        assert irods_store.dir_exists(remote_path, session)
        assert not irods_store.dir_exists(join(remote_base_path, "notCollection"), session)
    finally:
        clear_dir(testdir)
        # delete_collection(remote_path, token)


# @pytest.mark.skip(reason='debug')
def test_file_exists(remote_base_path):
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
        assert irods_store.file_exists(join(remote_path, file1_name), token)
        assert not irods_store.file_exists(join(remote_path, file2_name), token)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


# @pytest.mark.skip(reason='debug')
def test_list_directory(remote_base_path):
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
        files = irods_store.list_dir(remote_path, token)

        # check files
        assert join(remote_path, file1_name) in files
        assert join(remote_path, file2_name) in files
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


# @pytest.mark.skip(reason='debug')
def test_list_directory_no_retries_when_path_does_not_exist(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))
    with pytest.raises(ValueError):
        irods_store.list_dir(remote_path, token)


# @pytest.mark.skip(reason='debug')
def test_list_directory_retries_when_token_invalid(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))
    with pytest.raises(ValueError):
        irods_store.list_dir(remote_path, token)


# @pytest.mark.skip(reason='debug')
def test_download_file(remote_base_path):
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
        irods_store.pull_file(join(remote_path, file_name), testdir, token)

        # check download
        assert isfile(file_path)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)


# @pytest.mark.skip(reason='debug')
def test_download_directory(remote_base_path):
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
        irods_store.pull_dir(remote_path, testdir, ['.txt'], token)

        # check downloads
        assert isfile(file1_path)
        assert isfile(file2_path)
    finally:
        clear_dir(testdir)
        delete_collection(remote_path, token)
