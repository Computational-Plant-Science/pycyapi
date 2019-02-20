import os
from irods.session import iRODSSession

def sample_factory(name,props):
    '''
        name: name of sample
        props: {
            'storage': "irods" | "local"
            'path': 'path_to_sample'
        }
    '''
    if props['storage'] == 'irods':
        return iRODSSample(name,props['path'])

class Sample():

    def __init__(self, name, path):
        self.name = name
        self.path = path

    def get(self, path):
        pass

    def __str__(self):
        return "%s @ %s" % (self.name, self.path)

class iRODSSample(Sample):

    def __init__(self, name, path, env_file = None):
        super().__init__(name,path)

        if not env_file:
            try:
                self.env_file = os.environ['IRODS_ENVIRONMENT_FILE']
            except KeyError:
                self.env_file = os.path.expanduser('~/.irods/irods_environment.json')


    def get(self, to_path = "./"):
        session  = iRODSSession(irods_env_file=self.env_file)

        if session.data_objects.exists(self.path):
            local_path = os.path.join(to_path, os.path.basename(self.path))
            session.data_objects.get(self.path, file=local_path)
        elif session.collections.exists(self.path):
            if recursive:
                coll = session.collections.get(self.path)
                local_path = os.path.join(local_path, os.path.basename(self.path))
                os.mkdir(local_path)

                for file_object in coll.data_objects:
                    get(session, os.path.join(self.path, file_object.path), local_path, True)
                for collection in coll.subcollections:
                    get(session, collection.path, local_path, True)
            else:
                raise FileNotFoundError("Skipping directory " + self.path)
        else:
            raise FileNotFoundError(self.path + " Does not exist")

        return local_path
