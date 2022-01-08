import click

from plantit_cli.cli.base import terrain
from plantit_cli.store import terrain_commands


@terrain.command()
@click.argument('remote_path')
@click.option('--token', required=True, type=str)
@click.option('--local_path', '-p', required=False, type=str)
@click.option('--pattern', required=False, type=str, multiple=True)
@click.option('--overwrite', required=False, type=str, multiple=True)
def pull(
        remote_path,
        token,
        local_path,
        pattern,
        overwrite):
    terrain_commands.pull(
        remote_path=remote_path,
        token=token,
        local_path=local_path,
        patterns=list(pattern),
        overwrite=overwrite)


@terrain.command()
@click.argument('remote_path')
@click.option('--token', required=True, type=str)
@click.option('--local_path', '-p', required=False, type=str)
@click.option('--include_pattern', '-ip', required=False, type=str, multiple=True)
@click.option('--include_name', '-in', required=False, type=str, multiple=True)
@click.option('--exclude_pattern', '-ep', required=False, type=str, multiple=True)
@click.option('--exclude_name', '-en', required=False, type=str, multiple=True)
def push(remote_path,
         token,
         local_path,
         include_pattern,
         include_name,
         exclude_pattern,
         exclude_name):
    terrain_commands.push(
        local_path=local_path,
        token=token,
        remote_path=remote_path,
        include_patterns=include_pattern,
        include_names=include_name,
        exclude_patterns=exclude_pattern,
        exclude_names=exclude_name)
