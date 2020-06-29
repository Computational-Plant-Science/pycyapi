import os
import tempfile
import pytest

from irods.session import iRODSSession

from plantit_cluster.irods import IRODS, IRODSOptions

host = "irods"
port = 1247
user = "rods"
password = "rods"
zone = "tempZone"
data = "Hello, world!"
local_dir = tempfile.gettempdir()
remote_dir = f"/{zone}"


@pytest.fixture
def session():
    return iRODSSession(host=host,
                        port=port,
                        user=user,
                        password=password,
                        zone=zone)


@pytest.fixture
def irods():
    return IRODS(IRODSOptions(
        host=host,
        port=port,
        user=user,
        zone=zone,
        password=password))


def test_list(session, irods):
    local_file = tempfile.NamedTemporaryFile()
    local_path = local_file.name
    remote_file_path = os.path.join(remote_dir, local_file.name.split('/')[-1])
    remote_coll_path = os.path.join(remote_dir, "testCollection")

    try:
        session.collections.create(remote_coll_path)
        session.data_objects.put(local_path, remote_file_path)
        local_file.close()

        listed = irods.list(remote_dir)
        print(listed)

        assert remote_file_path.split('/')[-1] in listed
        assert remote_coll_path.split('/')[-1] in listed
    finally:
        session.data_objects.unlink(remote_file_path, force=True)
        session.collections.remove(remote_coll_path, force=True)


def test_irods_get(session, irods):
    local_file = tempfile.NamedTemporaryFile()
    local_path = local_file.name
    remote_path = os.path.join(remote_dir, local_file.name.split('/')[-1])

    try:
        with open(local_path, 'w') as file:
            file.write(data)

        session.data_objects.put(local_path, remote_path)
        local_file.close()

        irods.get(remote_path, local_dir)
        assert os.path.isfile(local_path)

        with open(local_path) as file:
            lines = file.readlines()
            assert len(lines) == 1
            assert lines[0] == data
    finally:
        session.data_objects.unlink(remote_path, force=True)


def test_irods_put(session, irods):
    local_file = tempfile.NamedTemporaryFile()
    local_path = local_file.name
    remote_path = os.path.join(remote_dir, local_file.name.split('/')[-1])

    try:
        with open(local_path, 'w') as file:
            file.write(data)

        irods.put(local_path, remote_path)
        local_file.close()

        session.data_objects.get(remote_path, local_dir)
        assert os.path.isfile(local_path)

        with open(local_path) as file:
            lines = file.readlines()
            assert len(lines) == 1
            assert lines[0] == data
    finally:
        session.data_objects.unlink(remote_path, force=True)
