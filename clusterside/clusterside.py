"""
    Bridge between the DIRT2 web platform and the cluster.
"""
import stat
import os
import subprocess
import argparse
import json
import sys

from clusterside.comms import Comms
from clusterside.executors import SingleJobExecutor
from process import SAMPLE_OUTPUT_TYPE

class ClusterSide:
    """
        Bridge between the DIRT2 web platform and the cluster.
    """

    # Configuration loaded form the Job config json file. Populated by main()
    config = {}

    # Server communication object
    server = None

    #

    def __init__(self):
        """
            Main Program Function
        """


        with open("workflow.json", 'r') as fin:
            self.config = json.load(fin)

        self.server = Comms(url=self.config['server_url'],
                            headers={
                                "Authorization": "Token "  + self.config['auth_token']
                            },
                            job_pk=self.config['job_pk'])

    def submit(self, script_template):
        """
            Submit a job to the cluster

            Args:
                args: command line input arguments to parse
        """
        script_name = "./submit_%d.sh"%(self.config['job_pk'],)
        print(script_name)
        with open(os.path.expanduser(script_template), 'r') as fin, open(script_name, 'w') as fout:
            for line in fin:
                fout.write(line.format(**self.config))
        os.chmod(script_name,
                 stat.S_IRUSR | stat.S_IXUSR)

        try:
            ret = subprocess.run(["qsub", script_name],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.PIPE)
        except FileNotFoundError as error:
            self.server.update_status(self.server.FAILED, str(error))
            print(error)
            exit()

        if ret.returncode == 0:
            self.server.update_status(self.server.OK, "Queued")

            if ret.stdout:
                print(ret.stdout)

        if ret.stderr:
            self.server.update_status(self.server.FAILED, ret.stderr)

    def run(self):
        """
            Run the job task
        """
        self.server.update_status(self.server.OK, "Running")

        executor = SingleJobExecutor()
        executor.process()
        executor.reduce(SAMPLE_OUTPUT_TYPE)

def cli():
    parser = argparse.ArgumentParser(
        description='The Cluster Side Component of the DIRT2 Webplatform'
    )
    parser.add_argument('cmd', type=str,
                         help='what to do.')
    args, unknownargs = parser.parse_known_args(sys.argv[1:])

    main = ClusterSide()

    if args.cmd == "submit":
        parser = argparse.ArgumentParser(description='Submit a job to the cluster')
        parser.add_argument('--script', type=str,
                            default="~/.clusterside/submit.sh",
                            help='Script template location')
        opts = parser.parse_args(unknownargs)

        main.submit(opts.script)
    elif args.cmd == "run":
        main.run()

if __name__ == "__main__":
    cli()
