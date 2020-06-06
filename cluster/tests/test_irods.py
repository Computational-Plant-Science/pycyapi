import tempfile

from irods.session import iRODSSession

from cluster.irods import IRODSOptions, IRODS

# TODO finish these


# def test_irods_download():
#     options = IRODSOptions('irods', 1247, 'rods', 'tempZone', 'rods')
#     session = iRODSSession(host=options.host, port=options.port, user=options.user,
#                            password=options.password, zone=options.zone)
#     irods = IRODS(options)
#     irods
#
#     local_file = tempfile.NamedTemporaryFile()
#     local_path = local_file.name
#     remote_file = session.data_objects.create(local_path)
#     with remote_file.open('r+') as f:
#         f.write(local_file.read())
#
#     actual = irods.download(remote_file.path)
#
#
# def test_irods_upload():
#     options = IRODSOptions('irods', 1247, 'rods', 'tempZone', 'rods')
#     session = iRODSSession(host=options.host, port=options.port, user=options.user,
#                            password=options.password, zone=options.zone)
#     irods = IRODS(options)
