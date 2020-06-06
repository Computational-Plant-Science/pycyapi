import subprocess

from dagster import solid

from clusterside.dagster.types import DagsterJob
from clusterside.exceptions import JobException


@solid
def sources(context, job: DagsterJob) -> DagsterJob:
    context.log.info(f"Successfully pulled from sources for job '{job.id}'")
    return job


@solid
def container(context, job: DagsterJob) -> DagsterJob:
    cmd = [
              "singularity",
              "exec",
              "--containall",
              job.container
          ] + job.commands
    context.log.info(f"Preparing to execute '{cmd}' for job '{job.id}'")
    ret = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if ret.returncode != 0:
        msg = f"Non-zero exit code from '{cmd}' for job '{job.id}': {ret.stderr.decode('utf-8') if ret.stderr else ret.stdout.decode('utf-8') if ret.stdout else 'Unknown error'}"
        context.log.error(msg)
        raise JobException(msg)
    else:
        context.log.info(f"Successfully executed '{cmd}' for job '{job.id}' with output: {ret.stdout.decode('utf-8')}")

    return job


@solid
def targets(context, job: DagsterJob) -> DagsterJob:
    context.log.info(f"Successfully pushed to targets for job '{job.id}'")
    return job
