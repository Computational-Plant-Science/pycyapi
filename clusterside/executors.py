'''
    Executes the workflow code.

    Executors are responsible for applying the workflow process function
    to each sample. Parallelizing that process, and combining the results
    from the analysis of each sample into a single results file (or zip).

    Currently, the only supported executor is :class:`SingleJobExecutor`
    which parallelizes sample processing over multiple CPU processes.

    To parallize sample processing over multiple cluster jobs,
    :class:`Executor` could be extended to submit these jobs, wait until
    they are complete, then combine the results as  :class:`SingleJobExecutor`
    does between processes.
'''
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
    '''
        Copy files from the processed samples into result_path/files
        to be returned to the user.

        Args:
            c (sql cursor): The sql connection cursor
            sample_path (str): path to the individual sample folders
            result_path (str): path to where to create the "files" directory
            to save the results.
    '''
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

def reduce_csv(c,result_file,key_order = None):
    '''
        Convert the key-pair values returned by the process_sample function
        calls for each sample into a csv file to be returned to the user.


        Args:
            c (sql cursor): The sql connection cursor
            result_file (str): filename/path of the file to to save the results in.
            key_order (list): Ordered list of keys. Columns in the csv will
                be ordered according the the order of the keys in `key_order`.
                If a key is not in `key_order`, it is placed at the end of the
                ordered columns.
    '''
    if not c.execute("SELECT * FROM key_val limit 1").fetchone():
        return False

    res = c.execute("SELECT DISTINCT key from key_val")
    column_headers = [key[0] for key in res]

    if(key_order):
        #Sort column order
        def sort_key(value):
            try:
                return key_order.index(value)
            except ValueError:
                return len(key_order)

        column_headers.sort(key=sort_key)

    column_headers.insert(0,'sample') #Add sample name column to beginning
    
    with open(result_file,'w') as outfile:
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
    '''
        The executor is responsible for running the `process_sample` function
        on each of the sample, reducing the results down, and uploading the
        results back to the irods server.

        Results from each `process_sample` call are saved in a separate
        folder and logged in the the results.sqlite database. The information
        in the sqlite database is then used to collate the results from
        all process calls to create the final zip file returned to the user.

        Attributes:
            collection (:class:`data.collection`): Collection data.
            workflow (:class:`data.workflow`): workflow data.
            server (:class:`comms.Comms`): The communcation object to use
                to communicate with the cluster.
            results_folder_path (str): The folder results files will be placed.
                (results_folder_path = abspath("./results"))
            sqlite (sqlite connection): Connection to sqlite results database
    '''
    def __init__(self,collection,workflow,server):
        '''
            Args:
                collection (:class:`data.collection`): Collection data.
                workflow (:class:`data.workflow`): workflow data.
                server (:class:`comms.Comms`): The communication object to use
                    to communicate with the cluster.
        '''
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
        '''
            Called to begin the execution of the workflow.

            This should be overridden by extending classes.

            The extending class must execute :meth:`execute` for each
            sample in :attr:`collection`. How `execute` is called is up to the
            extending class.  For example :class:`SingleJobExecutor` uses a
            parallel pool to run execute.
        '''
        raise NotImplementedError

    @staticmethod
    def execute(sample, workflow, server, results_folder_path):
        '''
            Runs the `process_sample` function in its given singularity container
            for for the given sample.

            Errors caused by running the process_sample function or
            while trying to run the singularity container are caught
            by execute and passed to the server as a WARN status message.

            Args:
                sample (:class:`data.Sample`): The sample to process.
                workflow (:class:`data.workflow`): workflow data.
                server (:class:`comms.Comms`): The communcation object to use
                    to communicate with the cluster.
                results_folder_path (str): The folder where results files
                    will be placed.
        '''

        start_dir = os.getcwd()
        workdir = sample.name

        server.update_status(server.OK, "Analyzing %s"%(sample))

        #make dir
        if not os.path.exists(workdir):
            os.mkdir(workdir)
        workdir = os.path.abspath(workdir)

        #Copy the sample to the dir
        try:
            sample_path = sample.get(workdir)
        except Exception as err:
            server.update_status(server.WARN,
                "Could not get sample %s: %s"%(sample.name,err))
            return

        #Create the params.json file
        params = {
                  'sample_name': sample.name,
                  'sample_path': sample_path,
                  'args': workflow.args
        }
        with open(os.path.join(workdir,'params.json'),'w') as fout:
            json.dump(params, fout)

        if(workflow.pre_commands):
            try:
                cmd = [s.format(workdir=workdir) for s in workflow.pre_commands]
                ret = subprocess.run(cmd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE)
            except Exception as error:
                server.update_status(server.WARN,
                    "Failed during running workflow.PRE_COMMANDS: " + str(error))
                return

            if ret.returncode != 0:
                if ret.stderr:
                    server.update_status(server.WARN, ret.stderr.decode("utf-8"))
                elif ret.stdout:
                    server.update_status(server.WARN, ret.stdout.decode("utf-8"))
                else:
                    server.update_status(server.WARN,
                                             "Unknown error occurred while running pre commands")
                return

        try:
            extra_flags = [s.format(workdir=workdir) for s in workflow.singularity_flags]
            ret = subprocess.run(["singularity",
                                  "exec"
                                  ] + extra_flags + [
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
            server.update_status(server.WARN,
                ("Could not start singularity (sample %s): "(sample.name,) +
                 str(error)))
            return

        if ret.returncode == 0:
            #Finished without error
            server.update_status(server.OK, "%s done."%(sample))
        else:
            if ret.stderr:
                server.update_status(server.WARN,
                    ("Exited with code != 0 (sample %s), "%(sample.name,) +
                     "stderr:" +
                     ret.stderr.decode("utf-8")))
            elif ret.stdout:
                server.update_status(server.WARN,
                    ("Exited with code != 0 (sample %s),"%(sample.name,) +
                     " no stderr, showing stdout" +
                     ret.stdout.decode("utf-8")))
            else:
                server.update_status(server.WARN,
                    ("Unknown error occurred while running process_sample," +
                     " (sample %s)"%(sample.name)))
            return

        if ret.stdout:
            with open("log.out", 'w') as fout:
                fout.write((" (sample %s)"%(sample.name) +
                            ret.stdout.decode("utf-8")))
        if ret.stderr:
            with open("log.err", 'w') as fout:
                fout.write((" (sample %s)"%(sample.name) +
                            ret.stderr.decode("utf-8")))

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
        reduce_csv(c,
                   join(folder_path,'results.csv'),
                   self.workflow.get('key_order',None))

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
