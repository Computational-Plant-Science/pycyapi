import json
from .samples import sample_factory

class Workflow():
    def __init__(self, json_file):
        with open(json_file,"r") as fin:
            params = json.load(fin)

        self.singularity_url = params['singularity_url']
        self.auth_token = params['auth_token']
        self.job_pk = params['job_pk']
        self.server_url = params['server_url']
        
        self.args = {}

class Collection():

    def __init__(self, json_file):
        with open(json_file,"r") as fin:
            self.__samples__ = json.load(fin)

    def samples(self):
        """
            Returns a generator of samples within the collection.
        """
        for name,sample in self.__samples__.items():
            yield sample_factory(name,sample)
