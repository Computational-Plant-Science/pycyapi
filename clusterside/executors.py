import os
import json
import subprocess
import csv
from multiprocessing import Pool, TimeoutError
from functools import partial

from .data import Workflow, Collection
from .comms import Comms

class Executor():

    def __init__(self,collection,workflow,server):
        self.collection = collection
        self.workflow = workflow
        self.server = server

        results_folder_path = "./results"
        if not os.path.exists(results_folder_path):
            os.mkdir(results_folder_path)
        self.results_folder_path = os.path.abspath(results_folder_path)

    def process(self):
        raise NotImplementedError

    @staticmethod
    def execute(sample, workflow, server, results_folder_path):
        # TODO: Fix issue where next sample workdir is placed in last sample workdir
        # if last sample failed.
        start_dir = os.getcwd()
        workdir = sample.name

        server.update_status(server.OK, "Analyzing %s"%(sample))

        #make dir
        if not os.path.exists(workdir):
            os.mkdir(workdir)
        workdir = os.path.abspath(workdir)

        #Copy the sample to the dir
        sample_path = sample.get(workdir)

        #Create the params.json file
        params = {
                  'sample_name': sample.name,
                  'sample_path': sample_path,
                  'args': workflow.args
        }
        with open(os.path.join(workdir,'params.json'),'w') as fout:
            json.dump(params, fout)

        #TODO: deal with self.server calls
        try:
            ret = subprocess.run(["singularity",
                                  "exec",
                                  "--containall",
                                  "--home", workdir,
                                  "--bind", "%s:/user_code/"%(os.getcwd()),
                                  "--bind", "%s:/bootstrap_code/"%(os.path.dirname(__file__)),
                                  "--bind", "%s:/results/"%(results_folder_path),
                                  workflow.singularity_url,
                                  "python3", "/bootstrap_code/bootstrapper.py"
                                 ],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        except Exception as error:
            server.update_status(server.FAILED, str(error))
            return

        if ret.returncode == 0:
            #Finished without error
            server.update_status(server.OK, "%s done."%(sample))
            pass
        else:
            if ret.stderr:
                server.update_status(server.FAILED, ret.stderr.decode("utf-8"))
            elif ret.stdout:
                server.update_status(server.FAILED, ret.stdout.decode("utf-8"))
            else:
                server.update_status(server.FAILED,
                                         "Unknown error occurred while running singularity")

        if ret.stdout:
            with open("log.out", 'w') as fout:
                fout.write(ret.stdout.decode("utf-8"))
        if ret.stderr:
            with open("log.err", 'w') as fout:
                fout.write(ret.stderr.decode("utf-8"))

    def reduce(self):
        if self.workflow.output_type == 'csv':
            self.reduce_csv()

    def reduce_csv(self):
        dir_files = [os.path.join(self.results_folder_path,file) for file in os.listdir(self.results_folder_path)]

        with open(dir_files[0],'r') as fin:
            data = json.load(fin)
            fieldnames = data.keys()

        with open('results.csv','w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for file in dir_files:
                with open(file,'r') as fin:
                    writer.writerow(json.load(fin))


class SingleJobExecutor(Executor):
    '''
        Processes samples using a pool of threaded workers
    '''
    def __init__(self, *args, processes = 2,  **kwargs):
        super().__init__(*args,**kwargs)
        self.processes = processes

    def process(self):
        pool = Pool(processes = self.processes)

        pool.map(partial(self.execute,
                         workflow=self.workflow,
                         server=self.server,
                         results_folder_path=self.results_folder_path),
                 self.collection.samples())
