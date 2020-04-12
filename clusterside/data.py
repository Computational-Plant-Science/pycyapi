"""
    Handling of communication with file servers.

    Currently only iRODS is supported.
"""
import json
import os
from irods.session import iRODSSession


def sample_factory(name, props):
    """
        Create a sample python object for a dictionary of sample
        properties.

        Currently, "irods" is the only valid storage type.

        Args:
            name (str): name of sample
            props (dict): {
                'storage': "irods"
                'path': 'path_to_sample'
            }
    """
    storage_type = props['storage']
    if storage_type == 'irods':
        return iRODSSample(name, props['path'])
    else:
        raise ValueError("'irods' is the only supported storage type, got %s" % storage_type)


class Sample:
    """
        A base sample object.

        Samples are to be processed by process_sample

        Attributes:
            name (str): sample name
            path (str): permanent storage path.
    """

    def __init__(self, name, path):
        self.name = name
        self.path = path

    def get(self, path):
        """
            Copy the sample file from its source file system.

            Args:
                path (str): where to place the sample on the local
                    filesystem.
        """
        pass

    def __str__(self):
        return "%s @ %s" % (self.name, self.path)


class iRODSSample(Sample):
    """
        An irods sample object.

        The sample is downloaded from irods before being processed by
        process_sample.
    """

    def __init__(self, name, path, env_file=None):
        super().__init__(name, path)

        if not env_file:
            try:
                self.env_file = os.environ['IRODS_ENVIRONMENT_FILE']
            except KeyError:
                self.env_file = os.path.expanduser('~/.irods/irods_environment.json')

    def get(self, to_path="./"):
        return self._copy(self.path, to_path)

    def _copy(self, from_path, to_path):
        """
            Copy a file/folder from the irods server

            If a folder path is given for the from_path, it is copied
            recursively.

            Args:
                from_path (str): folder path to which to copy the filer/folder
                to_path (str): folder path on local file system
                    to put the file/folder
        """
        session = iRODSSession(irods_env_file=self.env_file)

        # Collections can not end in a /
        from_path = from_path.rstrip("/")

        if session.data_objects.exists(from_path):
            local_path = os.path.join(to_path, os.path.basename(from_path))
            session.data_objects.get(from_path, file=local_path)
        elif session.collections.exists(from_path):
            coll = session.collections.get(from_path)
            base = from_path.split("/")[-1]
            local_path = os.path.join(to_path, base)
            os.mkdir(local_path)

            for file_object in coll.data_objects:
                self._copy(os.path.join(from_path, file_object.path),
                           local_path)
            for collection in coll.subcollections:
                self._copy(os.path.join(from_path, collection.path),
                           local_path)
        else:
            raise FileNotFoundError(self.path + " Does not exist")

        return local_path


class Workflow:
    """
        Contains information about the workflow to be run

        Attributes:
            output_type (str): the type of value returned by process_sample. See
                WORKFLOW_CONFIG docs for more info.
            singularity_url (str): the url to the singularity container that
                process sample is run in.
            auth_token (str): The authorization token required to communicate
                to the web server via the REST API
            job_pk (int): the job pk
            server_url (str): The url to the webserver REST api
            pre_commands (list): command line parameters to be run before
                the singularity container starts. Should be a list in the
                format used by subprocess.run.
            singularity_flags (list): flags added to the `singularity exec`
                command that runs the workflow's container. Should be a list
                in the format used by subprocess.run.
            args (dict): the arguments that are passed to process_sample
    """

    def __init__(self, json_file):
        with open(json_file, "r") as fin:
            params = json.load(fin)

        self.api_version = params['api_version']
        self.singularity_url = params['singularity_url']
        self.auth_token = params['auth_token']
        self.job_pk = params['job_pk']
        self.server_url = params['server_url']
        self.pre_commands = params.get('pre_commands', None)
        self.singularity_flags = params.get('singularity_flags', [])
        self.key_order = params.get('key_order', None)
        self.args = params['parameters']


class Collection:
    """
        Contains information about the collection of samples to be processed.
    """

    def __init__(self, json_file):
        """
            Args:
                json_file (str): path to the json file containing information
                    about the samples
        """
        with open(json_file, "r") as fin:
            data = json.load(fin)
            self.name = data['name']
            self.storage_type = data['storage_type']
            self.base_file_path = data['base_file_path']
            self.__samples__ = data['sample_set']

    def samples(self):
        """
            Returns a generator of samples within the collection.
        """
        for name, sample in self.__samples__.items():
            yield sample_factory(name, sample)


def upload_file(local_path, irods_path):
    """
        Test method for uploading results right back to irods
    """
    try:
        env_file = os.environ['IRODS_ENVIRONMENT_FILE']
    except KeyError:
        env_file = os.path.expanduser('~/.irods/irods_environment.json')

    session = iRODSSession(irods_env_file=env_file)

    if os.path.isfile(local_path):
        session.data_objects.put(local_path, irods_path)
    else:
        raise FileNotFoundError(local_path + " Does not exist")
