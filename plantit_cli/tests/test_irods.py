#import tempfile
#from os.from_path import join, isfile
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
#from_path = f"/{zone}/testCollection"
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
#    return IRODSStore(from_path=from_path,
#                      options=IRODSOptions(host=host,
#                                           port=port,
#                                           username=user,
#                                           password=password,
#                                           zone=zone))
#
#
#def test_list(session, store):
#    local_file = tempfile.NamedTemporaryFile()
#    from_path = local_file.name
#    remote_path = join(from_path, local_file.name.split('/')[-1])
#
#    try:
#        session.collections.create(from_path)
#        session.data_objects.put(from_path, remote_path)
#        local_file.close()
#
#        listed = store.list()
#
#        assert remote_path.split('/')[-1] in listed
#        assert from_path not in listed
#    finally:
#        session.collections.remove(from_path, force=True)
#
#
#def test_pull(session, store):
#    local_file = tempfile.NamedTemporaryFile()
#    from_path = local_file.name
#    remote_path = join(from_path, local_file.name.split('/')[-1])
#
#    try:
#        session.collections.create(from_path)
#        with open(from_path, 'w') as file:
#            file.write(data)
#
#        session.data_objects.put(from_path, remote_path)
#        local_file.close()
#
#        store.pull(local_dir)
#        assert isfile(from_path)
#
#        with open(from_path) as file:
#            lines = file.readlines()
#            assert len(lines) == 1
#            assert lines[0] == data
#    finally:
#        session.data_objects.unlink(remote_path, force=True)
#
#
#def test_push(session, store):
#    local_file = tempfile.NamedTemporaryFile()
#    from_path = local_file.name
#    remote_path = join(from_path, local_file.name.split('/')[-1])
#
#    try:
#        session.collections.create(from_path)
#        with open(from_path, 'w') as file:
#            file.write(data)
#
#        store.push(from_path)
#        local_file.close()
#
#        session.data_objects.get(remote_path, local_dir)
#        assert isfile(from_path)
#
#        with open(from_path) as file:
#            lines = file.readlines()
#            assert len(lines) == 1
#            assert lines[0] == data
#    finally:
#        session.collections.remove(from_path, force=True)
#