import os

from irods.session import iRODSSession


class IRODSOptions:
    def __init__(self, host: str, port: int, user: str, password: str, zone: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.zone = zone


class IRODS(object):
    def __init__(self, options: IRODSOptions):
        self.options = options

    def download(self, remote_path, local_path):
        session = iRODSSession(self.options.host, port=self.options.port, user=self.options.user,
                               password=self.options.password, zone=self.options.zone)
        remote_path = remote_path.rstrip("/")

        if session.data_objects.exists(remote_path):
            local_path = os.path.join(local_path, os.path.basename(remote_path))
            session.data_objects.get(remote_path, file=local_path)
        elif session.collections.exists(remote_path):
            coll = session.collections.get(remote_path)
            local_path = os.path.join(local_path, remote_path.split("/")[-1])
            os.mkdir(local_path)

            for file_object in coll.data_objects:
                self.download(os.path.join(remote_path, file_object.path), local_path)

            for collection in coll.subcollections:
                self.download(os.path.join(remote_path, collection.path), local_path)
        else:
            raise FileNotFoundError(f"'{remote_path}' does not exist")

        session.cleanup()

    def upload(self, local_path, remote_path):
        session = iRODSSession(self.options.host, port=self.options.port, user=self.options.user,
                               password=self.options.password, zone=self.options.zone)
        remote_path = remote_path.rstrip("/")

        file = session.data_objects.create(remote_path)
        with file.open('r+') as r:
            with open(local_path) as l:
                r.write(l.read())

        session.cleanup()
