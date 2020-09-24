#import tempfile
#from os.path import join, isfile
#
#import pytest
#from irods.session import iRODSSession
#
#from plantit_cli.store.irods import IRODSStore, IRODSOptions
#
#host = "irods"
#port = 1247
#user = "rods"
#password = "rods"
#zone = "tempZone"
#path = f"/{zone}/testCollection"
#data = "Hello, world!"
#local_dir = tempfile.gettempdir()
#
#
#@pytest.fixture
#def session():
#    return iRODSSession(host=host,
#                        port=port,
#                        user=user,
#                        password=password,
#                        zone=zone)
#
#
#@pytest.fixture
#def store():
#    return IRODSStore(path=path,
#                      options=IRODSOptions(host=host,
#                                           port=port,
#                                           username=user,
#                                           password=password,
#                                           zone=zone))
#
#
#def test_list(session, store):
#    local_file = tempfile.NamedTemporaryFile()
#    path = local_file.name
#    remote_path = join(path, local_file.name.split('/')[-1])
#
#    try:
#        session.collections.create(path)
#        session.data_objects.put(path, remote_path)
#        local_file.close()
#
#        listed = store.list()
#
#        assert remote_path.split('/')[-1] in listed
#        assert path not in listed
#    finally:
#        session.collections.remove(path, force=True)
#
#
#def test_pull(session, store):
#    local_file = tempfile.NamedTemporaryFile()
#    path = local_file.name
#    remote_path = join(path, local_file.name.split('/')[-1])
#
#    try:
#        session.collections.create(path)
#        with open(path, 'w') as file:
#            file.write(data)
#
#        session.data_objects.put(path, remote_path)
#        local_file.close()
#
#        store.pull(local_dir)
#        assert isfile(path)
#
#        with open(path) as file:
#            lines = file.readlines()
#            assert len(lines) == 1
#            assert lines[0] == data
#    finally:
#        session.data_objects.unlink(remote_path, force=True)
#
#
#def test_push(session, store):
#    local_file = tempfile.NamedTemporaryFile()
#    path = local_file.name
#    remote_path = join(path, local_file.name.split('/')[-1])
#
#    try:
#        session.collections.create(path)
#        with open(path, 'w') as file:
#            file.write(data)
#
#        store.push(path)
#        local_file.close()
#
#        session.data_objects.get(remote_path, local_dir)
#        assert isfile(path)
#
#        with open(path) as file:
#            lines = file.readlines()
#            assert len(lines) == 1
#            assert lines[0] == data
#    finally:
#        session.collections.remove(path, force=True)
#