import click

import pycyde.commands as commands


@click.group()
def cli():
    pass


@cli.command()
@click.option('--username', required=True, type=str)
@click.option('--password', required=True, type=str)
def token(username, password):
    return commands.token(username, password)


@cli.command()
@click.argument('remote_path')
@click.option('--local_path', '-p', required=False, type=str)
@click.option('--pattern', required=False, type=str, multiple=True)
@click.option('--overwrite', required=False, type=str, multiple=True)
@click.option('--token', required=False, type=str)
def pull(
        remote_path,
        local_path,
        pattern,
        overwrite,
        token):
    commands.pull(
        remote_path=remote_path,
        local_path=local_path,
        patterns=list(pattern),
        overwrite=overwrite,
        token=token)


@cli.command()
@click.argument('remote_path')
@click.option('--local_path', '-p', required=False, type=str)
@click.option('--include_pattern', '-ip', required=False, type=str, multiple=True)
@click.option('--include_name', '-in', required=False, type=str, multiple=True)
@click.option('--exclude_pattern', '-ep', required=False, type=str, multiple=True)
@click.option('--exclude_name', '-en', required=False, type=str, multiple=True)
@click.option('--token', required=False, type=str)
def push(remote_path,
         local_path,
         include_pattern,
         include_name,
         exclude_pattern,
         exclude_name,
         token):
    commands.push(
        local_path=local_path,
        remote_path=remote_path,
        include_patterns=include_pattern,
        include_names=include_name,
        exclude_patterns=exclude_pattern,
        exclude_names=exclude_name,
        token=token)
