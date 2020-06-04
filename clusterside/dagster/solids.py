import subprocess
import traceback

from dagster import solid

from clusterside.comms import RESTComms, STDOUTComms
from clusterside.dagster.types import DagsterJob
from clusterside.exceptions import JobException


@solid
def sources(context, job: DagsterJob) -> DagsterJob:
    context.log.info(f"Successfully pulled from sources for job '{job.id}'")
    return job


@solid
def container(context, job: DagsterJob) -> DagsterJob:
    server = STDOUTComms() if not job.server else RESTComms(url=job.server,
                                                            headers={"Authorization": "Token " + job.token})

    try:
        cmd = [
                  "singularity",
                  "exec",
                  "--containall",
                  job.container
              ] + job.commands
        context.log.info(f"Preparing to execute '{cmd}'")
        ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if ret.returncode != 0:
            msg = f"Non-zero exit code while running job '{job.id}': {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
            context.log.error(msg)
            server.update_status(job.id, job.token, server.FAILED, msg)
            raise JobException(msg)
        else:
            msg = f"Successfully ran job '{job.id}' with output: {ret.stdout.decode('utf-8')}"
            context.log.info(msg)
            server.update_status(job.id, job.token, server.OK, msg)

    except Exception as error:
        msg = f"Failed to run job '{job.id}': {traceback.format_exc()}"
        context.log.error(msg)
        server.update_status(job.id, job.token, server.FAILED, msg)
        raise error

    return job


@solid
def targets(context, job: DagsterJob) -> DagsterJob:
    context.log.info(f"Successfully pushed to targets for job '{job.id}'")
    return job
