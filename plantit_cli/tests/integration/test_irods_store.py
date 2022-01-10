import os
import uuid
from os.path import join, isfile
from tempfile import TemporaryDirectory

import pytest
from irods.collection import iRODSCollection
from irods.models import Collection
from irods.session import iRODSSession
from irods.ticket import Ticket

from plantit_cli.store import irods_store
from plantit_cli.tests.integration.test_utils import delete_collection, upload_file, create_collection
from plantit_cli.tests.utils import TerrainToken, TerrainTicket

message = "Message!"
token = TerrainToken.get()


def test_directory_exists(remote_base_path):
    remote_path = join(remote_base_path, str(uuid.uuid4()))

    with TemporaryDirectory() as testdir:
        try:
            # prep collection
            create_collection(remote_path, token)

            # create iRODS session
            session = iRODSSession(host='data.cyverse.org', port=1247, user='anonymous', password='', zone='iplant')
            ticket = TerrainTicket.get([remote_path])
            Ticket(session, ticket).supply()

            # test remote directories exist
            assert irods_store.dir_exists(remote_path, session)
            assert not irods_store.dir_exists(join(remote_base_path, "not_a_collection"), session)
        finally:
            delete_collection(remote_path, token)


@pytest.mark.skip(reason="ticket won't be granted if file doesn't exist")
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

            # create iRODS session
            session = iRODSSession(host='data.cyverse.org', port=1247, user='anonymous', password='', zone='iplant')
            ticket = TerrainTicket.get([join(remote_path, file1_name)], uses=2)
            Ticket(session, ticket).supply()

            # test remote files exist
            assert irods_store.file_exists(join(remote_path, file1_name), session)
            assert not irods_store.file_exists(join(remote_path, file2_name), session)
        finally:
            delete_collection(remote_path, token)


def test_list_directory(remote_base_path):
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

            # create iRODS session
            session = iRODSSession(host='data.cyverse.org', port=1247, user='anonymous', password='', zone='iplant')
            ticket = TerrainTicket.get([remote_path])
            Ticket(session, ticket).supply()

            # list files
            files = irods_store.list_dir(remote_path, session)

            # check files
            assert join(remote_path, file1_name) in files
            assert join(remote_path, file2_name) in files
        finally:
            delete_collection(remote_path, token)


def test_download_file(remote_base_path):
    with TemporaryDirectory() as testdir:
        file_name = 'f1.txt'
        file_path = join(testdir, file_name)
        remote_path = join(remote_base_path, str(uuid.uuid4()))
        local_path = join(testdir, f"d{file_name}")

        try:
            # prep collection
            create_collection(remote_path, token)

            # create files
            with open(file_path, "w") as file:
                file.write('Hello, 1!')

            # upload files
            upload_file(file_path, remote_path, token)

            # create iRODS session and get ticket
            with iRODSSession(host='data.cyverse.org', port=1247, user='anonymous', password='', zone='iplant') as session:
                ticket = TerrainTicket.get([remote_path], 'write', False)
                Ticket(session, ticket).supply()

                # download file
                irods_store.pull_file(join(remote_path, file_name), local_path, session=session)

            # check download
            assert isfile(local_path)
        finally:
            delete_collection(remote_path, token)


def test_download_directory(remote_base_path):
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

            # remove local files
            os.remove(file1_path)
            os.remove(file2_path)

            # create iRODS session and get ticket
            with iRODSSession(host='data.cyverse.org', port=1247, user='anonymous', password='', zone='iplant') as session:
                ticket = TerrainTicket.get([remote_path], 'write', False)
                Ticket(session, ticket).supply()

                # download files
                irods_store.pull_dir(remote_path, testdir, session=session, patterns=['.txt'])

            # check downloads
            assert isfile(file1_path)
            assert isfile(file2_path)
        finally:
            delete_collection(remote_path, token)


@pytest.mark.skip(reason="waiting for resolution: https://github.com/irods/python-irodsclient/issues/327")
def test_upload_file(remote_base_path):
    pass


@pytest.mark.skip(reason="waiting for resolution: https://github.com/irods/python-irodsclient/issues/327")
def test_upload_directory(remote_base_path):
    with TemporaryDirectory() as testdir:
        file1_name = 'f1.txt'
        file2_name = 'f2.txt'
        file1_path = join(testdir, file1_name)
        file2_path = join(testdir, file2_name)
        coll_name = str(uuid.uuid4())
        remote_path = join(remote_base_path, coll_name)

        try:
            # prep collection
            create_collection(remote_path, token)

            # create files
            with open(file1_path, "w") as file1, open(file2_path, "w") as file2:
                file1.write('Hello, 1!')
                file2.write('Hello, 2!')

            # create iRODS session and get ticket
            with iRODSSession(host='data.cyverse.org', port=1247, user='anonymous', password='', zone='iplant') as session:
                ticket = TerrainTicket.get([join(remote_path, file1_name), join(remote_path, file2_name)], 'write', False)
                Ticket(session, ticket).supply()

                # upload files
                irods_store.push_dir(testdir, remote_path, session=session, include_patterns=['.txt'])

                # check uploads
                coll = session.query(Collection).one()
                collection = iRODSCollection(session.collections, coll)
                file_names = [o.name for o in collection.data_objects]
                print(file_names)
                assert file1_name in file_names
                assert file2_name in file_names
        finally:
            delete_collection(remote_path, token)
