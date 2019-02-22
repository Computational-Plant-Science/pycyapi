import os
import json
import subprocess
import csv
from multiprocessing import Pool, TimeoutError
from functools import partial

from .data import Workflow, Collection
from .comms import Comms

def execute(sample, workflow, server, results_folder_path):
    # TODO: Fix issue where next sample workdir is placed in last sample workdir
    # if last sample failed.
    start_dir = os.getcwd()
    workdir = sample.name

    self.server.update_status(self.server.OK, "Analyzing %s"%(sample))

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
        server.update_status(self.server.FAILED, str(error))
        return

    if ret.returncode == 0:
        #Finished without error
        self.server.update_status(self.server.OK, "%s done."%(sample))
        pass
    else:
        if ret.stderr:
            server.update_status(self.server.FAILED, ret.stderr.decode("utf-8"))
        elif ret.stdout:
            server.update_status(self.server.FAILED, ret.stdout.decode("utf-8"))
        else:
            server.update_status(self.server.FAILED,
                                     "Unknown error occurred while running singularity")

    if ret.stdout:
        with open("run_%d_%s.out"%(self.config['job_pk'],sample), 'w') as fout:
            fout.write(ret.stdout.decode("utf-8"))
    if ret.stderr:
        with open("run_%d_%s.err"%(self.config['job_pk'],sample), 'w') as fout:
            fout.write(ret.stderr.decode("utf-8"))


class SingleJobExecutor():

    def __init__(self):
        self.collection = Collection("samples.json")
        self.workflow = Workflow("workflow.json")
        self.server = Comms(url=self.workflow.server_url,
                            headers={
                                "Authorization": "Token "  + self.workflow.auth_token
                            },
                            job_pk=self.workflow.job_pk)

        results_folder_path = "./results"
        if not os.path.exists(results_folder_path):
            os.mkdir(results_folder_path)
        self.results_folder_path = os.path.abspath(results_folder_path)

    def process(self):
        pool = Pool(processes = 2)

        pool.map(partial(execute,
                         workflow=self.workflow,
                         server=self.server,
                         results_folder_path=self.results_folder_path),
                 self.collection.samples())

    def reduce(self,output_type):
        if output_type == 'csv':
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
