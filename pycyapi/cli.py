import importlib

import click

import pycyapi
from pycyapi.cyverse import commands as cyverse_commands


@click.group()
def cli():
    pass


@cli.command()
def version():
    click.echo(pycyapi.__version__)


@cli.command()
@click.option("--username", required=True, type=str)
@click.option("--password", required=True, type=str)
def token(username, password):
    click.echo(
        cyverse_commands.cas_token(username=username, password=password)
    )


@cli.command()
@click.argument("username")
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def user(username, token, timeout):
    click.echo(
        cyverse_commands.user_info(
            username=username, token=token, timeout=timeout
        )
    )


@cli.command()
@click.argument("remote_path")
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def list(remote_path, token, timeout):
    click.echo(
        cyverse_commands.paged_directory(
            path=remote_path, token=token, timeout=timeout
        )
    )


@cli.command()
@click.argument("remote_path")
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def stat(remote_path, token, timeout):
    click.echo(
        cyverse_commands.stat(path=remote_path, token=token, timeout=timeout)
    )


@cli.command()
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


@cli.command()
@click.argument("remote_path")
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def create(remote_path, token, timeout):
    click.echo(
        cyverse_commands.create(path=remote_path, token=token, timeout=timeout)
    )


@cli.command()
@click.argument("remote_path")
@click.option("--local_path", "-p", required=False, type=str)
@click.option(
    "--include_pattern", "-ip", required=False, type=str, multiple=True
)
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


@cli.command()
@click.argument("remote_path")
@click.option("--local_path", "-p", required=False, type=str)
@click.option(
    "--include_pattern", "-ip", required=False, type=str, multiple=True
)
@click.option("--include_name", "-in", required=False, type=str, multiple=True)
@click.option(
    "--exclude_pattern", "-ep", required=False, type=str, multiple=True
)
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


@cli.command()
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


@cli.command()
@click.argument("remote_path")
@click.option("--username", "-u", required=True, type=str, multiple=True)
@click.option("--token", "-t", required=False, type=str)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def unshare(remote_path, username, token, timeout):
    cyverse_commands.unshare(
        username=username, path=remote_path, token=token, timeout=timeout
    )
    click.echo(f"Unshared {remote_path} with {username}")


@cli.command()
@click.argument("id")
@click.option("--attribute", "-a", required=False, type=str, multiple=True)
@click.option(
    "--irods_attribute", "-ia", required=False, type=str, multiple=True
)
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


@cli.command()
@click.argument("id")
@click.option("--token", "-t", required=False, type=str)
@click.option("--irods", "-i", required=False, default=False, type=bool)
@click.option("--timeout", "-to", required=False, type=int, default=15)
def tags(id, irods, token, timeout):
    attributes = cyverse_commands.tags(
        id=id, irods=irods, token=token, timeout=timeout
    )
    newline = "\n"
    click.echo(newline.join(attributes))
