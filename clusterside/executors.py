import os
from os.path import join, basename, abspath
import json
import subprocess
import csv
import sqlite3
import zipfile
import shutil

from multiprocessing import Pool, TimeoutError
from functools import partial

from .data import Workflow, Collection
from .comms import Comms

def reduce_files(c,sample_path,result_path):
    if not c.execute("SELECT * FROM files limit 1").fetchone():
        return False

    res = c.execute(
           '''SELECT name,file_path FROM samples
              INNER JOIN files ON samples.id = files.sample'''
           )

    os.mkdir(join(result_path,'files'))

    for file in res:
        sample_name = file[0]
        file_path = file[1]

        shutil.move(join(sample_path,sample_name,file_path),
                    join(result_path,
                         'files',
                         '%s_%s'%(sample_name,basename(file_path))))

    return True

def reduce_csv(c,result_path):
    if not c.execute("SELECT * FROM key_val limit 1").fetchone():
        return False

    res = c.execute("SELECT DISTINCT key from key_val")
    column_headers = ['sample'] + [key[0] for key in res]

    with open(result_path,'w') as outfile:
        writer = csv.DictWriter(outfile,fieldnames = column_headers, restval='NULL')
        writer.writeheader()

        for s in  c.execute("SELECT name FROM samples").fetchall():
            sample = s[0]

            res = c.execute(
                   '''SELECT key,data FROM samples
                      INNER JOIN key_val ON samples.id = key_val.sample
                      WHERE samples.name = ?''',(sample,)
                   )

            row = {}
            row['sample'] = sample
            for key_val in res:
                row[key_val[0]] = key_val[1]
            writer.writerow(row)

    return True

class Executor():

    def __init__(self,collection,workflow,server):
        self.collection = collection
        self.workflow = workflow
        self.server = server

        results_folder_path = "./results"
        if not os.path.exists(results_folder_path):
            os.mkdir(results_folder_path)
        self.results_folder_path = os.path.abspath(results_folder_path)

        self.sqlite = sqlite3.connect(join(results_folder_path,'results.sqlite'))
        c = self.sqlite.cursor()
        c.execute('''
                  CREATE TABLE samples(
                    id INTEGER PRIMARY KEY,
                    name TEXT
                  );
        ''')
        c.execute('''
                  CREATE TABLE files(
                    file_path TEXT,
                    sample INTEGER,
                    FOREIGN KEY(sample) REFERENCES samples(id)
                  );
        ''')
        c.execute('''
                  CREATE TABLE key_val(
                    key TEXT,
                    data TEXT,
                    sample INTEGER,
                    FOREIGN KEY(sample) REFERENCES samples(id)
                  );
        ''')
        self.sqlite.commit()
        c.close()

    def __del__(self):
        self.sqlite.close()

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
        '''
            Collects the data in the sqllite database into a single file.

            Returns (str): absolute path to the collated results.
        '''
        c = self.sqlite.cursor()

        #Check for files
        folder_path = join(self.results_folder_path,'results')
        os.mkdir(folder_path)

        includes_files = reduce_files(c,os.getcwd(),folder_path)
        reduce_csv(c,join(folder_path,'results.csv'))

        if includes_files:
            shutil.make_archive('results', 'zip', folder_path)
            return abspath('./results.zip')
        else:
            return join(folder_path,'results.csv')

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
