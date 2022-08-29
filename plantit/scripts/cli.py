import re
import uuid
from pathlib import Path

import click
import yaml

import plantit.scripts.commands as commands
from plantit.scripts.models import ScriptConfig, JobqueueConfig


@click.command()
@click.argument("file", required=False)
@click.option("--image", required=False)
@click.option("--entrypoint", required=False)
@click.option("--workdir", required=False)
@click.option("--email", required=False)
@click.option("--guid", required=False)
@click.option("--token", required=False)
@click.option("--shell", required=False)
@click.option("--source", required=False)
@click.option("--sink", required=False)
@click.option("--input", required=False)
@click.option("--output", required=False)
@click.option("--parallel", required=False)
@click.option("--iterations", required=False)
@click.option("--environment", required=False, multiple=True)
@click.option("--bind_mounts", required=False)
@click.option("--log_file", required=False)
@click.option("--no_cache", required=False)
@click.option("--gpus", required=False)
@click.option("--walltime", required=False)
@click.option("--queue", required=False)
@click.option("--project", required=False)
@click.option("--mem", required=False)
@click.option("--nodes", required=False)
@click.option("--cores", required=False)
@click.option("--tasks", required=False)
@click.option("--processes", required=False)
def scripts(
        file,
        image,
        entrypoint,
        workdir,
        email,
        guid,
        token,
        shell,
        source,
        sink,
        input,
        output,
        iterations,
        environment,
        bind_mounts,
        no_cache,
        gpus,
        # jobqueue parameters
        walltime,
        queue,
        project,
        mem,
        nodes,
        cores,
        tasks,
        processes,
        header_skip,
        parallel_strategy,
):
    if file:
        if not Path(file).is_file():
            raise ValueError(f"Invalid path to configuration file: {file}")
        # parse config from file
        with open(file, 'r') as f:
            config = ScriptConfig(**yaml.safe_load(f))
    else:
        # make sure we have required options
        if not (image and
                entrypoint and
                workdir and
                email):
            raise ValueError("Missing required options "
                             "(need at least image, entrypoint, workdir, email)")

        # create GUID if none provided
        if not guid:
            guid = str(uuid.uuid4())
        elif not re.match(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', guid):
            raise ValueError(f"'{guid}' is not a valid GUID")

        # create config
        jobqueue = JobqueueConfig(
            walltime=walltime,
            queue=queue,
            project=project,
            mem=mem,
            nodes=nodes,
            cores=cores,
            tasks=tasks,
            processes=processes,
            header_skip=header_skip,
            parallel=parallel_strategy
        )
        config = ScriptConfig(
            image=image,
            entrypoint=entrypoint,
            workdir=workdir,
            email=email,
            guid=guid,
            token=token,
            shell=shell,
            source=source,
            sink=sink,
            input=input,
            output=output,
            iterations=iterations,
            environment=environment,
            bind_mounts=bind_mounts,
            no_cache=no_cache,
            gpus=gpus,
            jobqueue=jobqueue
        )

    # generate scripts
    script_paths = commands.scripts(config)
    script_names = [p.name for p in script_paths]
    click.echo(f"Generated job script(s): {', '.join(script_names)}")
