import concurrent.futures
import subprocess
import os
import logging
import tornado
import tornado.web
import tornado.ioloop
import json

from clusterside.workflow import Workflow

logger = logging.getLogger('plantit-clusterside-test')
logMessages = []


class Handler(tornado.web.RequestHandler):
    def post(self):
        logMessages.append(self.get_body_argument("task_set"))
        self.write(f"Received log message '{self.get_body_argument('task_set')}'")


def loggers():
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s'))
    logger.addHandler(handler)
    tornadoLogger = logging.getLogger("tornado.access")
    tornadoLogger.propagate = False
    tornadoLogger.setLevel(logging.DEBUG)
    tornadoLogger.handlers = [handler]


def run_tornado():
    loggers()
    tornado.web.Application([(r"/test", Handler)]).listen(4141)
    tornado.ioloop.IOLoop.current().start()


workflow_definition = '''
{
    "job_pk": 2,
    "token": "some_secret_value",
	"server_url": "http://localhost:4141/test",
    "container_url": "docker://python:3.6-stretch",
	"parameters": {
	    "output_file": "output.txt"
	},
    "pre_commands": null,
    "post_commands": null,
    "commands": "python3 --version > $OUTPUT_FILE",
    "flags": []
}
'''

with open("workflow.json", "w") as fout:
    fout.write(workflow_definition)

workflow = Workflow("workflow.json")

with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    server = executor.submit(run_tornado)
    subprocess.run("clusterside local")

os.remove("workflow.json")
