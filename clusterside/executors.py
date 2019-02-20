import os
import json
import subprocess
from multiprocessing import Pool, TimeoutError
from functools import partial

from .data import Workflow, Collection


def process_sample(sample,workflow, workdir, res_path):
    #TODO: deal with self.server calls
    try:
        ret = subprocess.run(["singularity",
                              "exec",
                              "--containall",
                              "--home", workdir,
                              "--bind", "%s:/user_code/"%(os.getcwd()),
                              "--bind", "%s:/bootstrap_code/"%(os.path.dirname(__file__)),
                              "--bind", "%s:/results/"%(res_path),
                              workflow.singularity_url,
                              "python3", "/bootstrap_code/bootstrapper.py"
                             ],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
    except Exception as error:
        print("¯\_(ツ)_/¯: Error occurred while trying to start singularity")
        print("     " + str(error))
        return
        #self.server.update_status(self.server.FAILED, str(error))

    if ret.returncode == 0:
        #self.server.task_complete(self.config['task_pk'])
        print("Sample %s finished successfully" % (sample))
    else:
        if ret.stderr:
            print("Sample %s finished with error: \" %s \"" % (sample,ret.stderr.decode("utf-8")))
            #self.server.update_status(self.server.FAILED, ret.stderr.decode("utf-8"))
        elif ret.stdout:
            print("Sample %s finished with error: \" %s \"" % (ret.stdout.decode("utf-8")))
            #self.server.update_status(self.server.FAILED, ret.stdout.decode("utf-8"))
        else:
            #self.server.update_status(self.server.FAILED,
            #                          "Unknown error occurred while running singularity")
            print("¯\_(ツ)_/¯: Unknown error occurred while running singularity")

    # if ret.stdout:
    #     with open("run_%d.out"%(self.config['job_pk'],), 'w') as fout:
    #         fout.write(ret.stdout.decode("utf-8"))
    # if ret.stderr:
    #     with open("run_%d.err"%(self.config['job_pk'],), 'w') as fout:
    #         fout.write(ret.stderr.decode("utf-8"))

def execute(sample, workflow, results_folder_path):
    # TODO: Fix issue where next sample workdir is placed in last sample workdir
    # if last sample failed.
    start_dir = os.getcwd()
    workdir = sample.name

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

    #Run the workflow code
    process_sample(sample, workflow, workdir, results_folder_path)


class SingleJobExecutor():

    def __init__(self):
        self.collection = Collection("samples.json")
        self.workflow = Workflow("workflow.json")

    def process(self):
        results_folder_path = "./results"
        if not os.path.exists(results_folder_path):
            os.mkdir(results_folder_path)
        results_folder_path = os.path.abspath(results_folder_path)

        pool = Pool(processes = 2)

        pool.map(partial(execute,
                         workflow=self.workflow,
                         results_folder_path= results_folder_path),
                 self.collection.samples())
