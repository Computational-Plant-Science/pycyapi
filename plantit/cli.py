import re
import uuid
from pathlib import Path

import click
import yaml

from plantit.cyverse import commands as cyverse_commands
from plantit.scripts import commands as script_commands
from plantit.scripts.models import JobqueueConfig, ScriptConfig
from plantit.submit import commands as submit_commands
from plantit.submit.models import SubmitConfig


@click.group
@click.argument("file", required=False)
def cli(ctx, file):
    if ctx.invoked_subcommand:
        return

    # make sure we have a config file
    if not file or not Path(file).is_file():
        raise ValueError(f"Invalid path to configuration file: {file}")

    # parse config
    with open(file, "r") as f:
        yml = yaml.safe_load(f)
        script_config = ScriptConfig(**yml)
        submit_config = SubmitConfig(**yml)

    # generate job script(s)
    script_paths = script_commands.scripts(script_config)
    script_names = [p.name for p in script_paths]
    click.echo(f"Generated job script(s): {', '.join(script_names)}")

    # submit job(s)
    job_ids = submit_commands.submit(submit_config)
    click.echo(f"Submitted job(s): {', '.join(job_ids)}")


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
        with open(file, "r") as f:
            config = ScriptConfig(**yaml.safe_load(f))
    else:
        # make sure we have required options
        if not (image and entrypoint and workdir and email):
            raise ValueError(
                "Missing required options "
                "(need at least image, entrypoint, workdir, email)"
            )

        # create GUID if none provided
        if not guid:
            guid = str(uuid.uuid4())
        elif not re.match(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", guid
        ):
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
            parallel=parallel_strategy,
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
            jobqueue=jobqueue,
        )

    # generate scripts
    script_paths = script_commands.scripts(config)
    script_names = [p.name for p in script_paths]
    click.echo(f"Generated job script(s): {', '.join(script_names)}")


@click.command()
def submit():
    click.echo("not implemented yet")


@click.command()
@click.option("--username", required=True, type=str)
@click.option("--password", required=True, type=str)
def token(username, password):
    click.echo(cyverse_commands.cas_token(username=username, password=password))


@click.command()
@click.argument("username")
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def user(username, token, timeout):
    click.echo(
        cyverse_commands.user_info(username=username, token=token, timeout=timeout)
    )


@click.command()
@click.argument("remote_path")
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def list(remote_path, token, timeout):
    click.echo(
        cyverse_commands.paged_directory(path=remote_path, token=token, timeout=timeout)
    )


@click.command()
@click.argument("remote_path")
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def stat(remote_path, token, timeout):
    click.echo(cyverse_commands.stat(path=remote_path, token=token, timeout=timeout))


@click.command()
@click.argument("remote_path")
@click.option("--type", required=False, type=str)
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def exists(remote_path, type, token, timeout):
    click.echo(
        cyverse_commands.exists(
            path=remote_path, type=type, token=token, timeout=timeout
        )
    )


@click.command()
@click.argument("remote_path")
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def create(remote_path, token, timeout):
    click.echo(cyverse_commands.create(path=remote_path, token=token, timeout=timeout))


@click.command()
@click.argument("remote_path")
@click.option("--local_path", "-p", required=False, type=str)
@click.option("--include_pattern", "-ip", required=False, type=str, multiple=True)
@click.option("--force", "-f", required=False, type=str, multiple=True)
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def pull(remote_path, local_path, include_pattern, force, token, timeout):
    cyverse_commands.download(
        remote_path=remote_path,
        local_path=local_path,
        patterns=include_pattern,
        force=force,
        token=token,
        timeout=timeout,
    )
    click.echo(f"Downloaded {remote_path} to {local_path}")


@click.command()
@click.argument("remote_path")
@click.option("--local_path", "-p", required=False, type=str)
@click.option("--include_pattern", "-ip", required=False, type=str, multiple=True)
@click.option("--include_name", "-in", required=False, type=str, multiple=True)
@click.option("--exclude_pattern", "-ep", required=False, type=str, multiple=True)
@click.option("--exclude_name", "-en", required=False, type=str, multiple=True)
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def push(
    remote_path,
    local_path,
    include_pattern,
    include_name,
    exclude_pattern,
    exclude_name,
    token,
    timeout,
):
    cyverse_commands.upload(
        local_path=local_path,
        remote_path=remote_path,
        include_patterns=include_pattern,
        include_names=include_name,
        exclude_patterns=exclude_pattern,
        exclude_names=exclude_name,
        token=token,
        timeout=timeout,
    )
    click.echo(f"Uploaded {local_path} to {remote_path}")


@click.command()
@click.argument("remote_path")
@click.option("--username", "-u", required=True, type=str)
@click.option("--permission", "-p", required=True, type=str)
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def share(remote_path, username, permission, token, timeout):
    cyverse_commands.share(
        username=username,
        path=remote_path,
        permission=permission,
        token=token,
        timeout=timeout,
    )
    click.echo(f"Shared {remote_path} with {username}")


@click.command()
@click.argument("remote_path")
@click.option("--username", "-u", required=True, type=str, multiple=True)
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def unshare(remote_path, username, token, timeout):
    cyverse_commands.unshare(
        username=username, path=remote_path, token=token, timeout=timeout
    )
    click.echo(f"Unshared {remote_path} with {username}")


@click.command()
@click.argument("id")
@click.option("--attribute", "-a", required=False, type=str, multiple=True)
@click.option("--irods_attribute", "-ia", required=False, type=str, multiple=True)
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def tag(id, attribute, irods_attribute, token, timeout):
    cyverse_commands.tag(
        id=id,
        attributes=attribute,
        irods_attributes=irods_attribute,
        token=token,
        timeout=timeout,
    )
    newline = "\n"
    click.echo(
        f"Tagged data object with ID {id}:\nRegular:\n{newline.join(attribute)}\niRODS:\n{newline.join(irods_attribute)}"
    )


@click.command()
@click.argument("id")
@click.option("--token", "-t", required=False, type=str)
@click.option("--irods", "-i", required=False, default=False, type=bool)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def tags(id, irods, token, timeout):
    attributes = cyverse_commands.tags(id=id, irods=irods, token=token, timeout=timeout)
    newline = "\n"
    click.echo(newline.join(attributes))
