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


workflow_definition = {
    "job_pk": 2,
    "token": "some_secret_value",
    "server_url": "http://localhost:4141/test",
    "container_url": "docker://python:3.6-stretch",
    "pre_commands": None,
    "post_commands": None,
    "commands": "python3 --version",
    "flags": []
}

workflow = Workflow(
    job_pk=workflow_definition['job_pk'],
    token=workflow_definition['token'],
    server_url=workflow_definition['server_url'],
    container_url=workflow_definition['container_url'],
    commands=workflow_definition['commands'],
    pre_commands=workflow_definition['pre_commands'],
    post_commands=workflow_definition['post_commands'],
    flags=workflow_definition['flags']
)

with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
    server = executor.submit(run_tornado)
    job = executor.submit(lambda _: subprocess.run("clusterside local"))

os.remove("workflow.json")
