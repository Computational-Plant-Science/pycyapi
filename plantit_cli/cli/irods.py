import click

from plantit_cli.cli.base import irods
from plantit_cli.store import irods_commands


@irods.command()
@click.argument('remote_path')
@click.option('--ticket', required=True, type=str)
@click.option('--local_path', '-p', required=False, type=str)
@click.option('--pattern', required=False, type=str, multiple=True)
@click.option('--overwrite', required=False, type=str, multiple=True)
def pull(
        remote_path,
        ticket,
        local_path,
        pattern,
        overwrite):
    irods_commands.pull(
        remote_path=remote_path,
        ticket=ticket,
        local_path=local_path,
        patterns=list(pattern),
        overwrite=overwrite)


@irods.command()
@click.argument('remote_path')
@click.option('--ticket', required=True, type=str)
@click.option('--local_path', '-p', required=False, type=str)
@click.option('--include_pattern', '-ip', required=False, type=str, multiple=True)
@click.option('--include_name', '-in', required=False, type=str, multiple=True)
@click.option('--exclude_pattern', '-ep', required=False, type=str, multiple=True)
@click.option('--exclude_name', '-en', required=False, type=str, multiple=True)
def push(remote_path,
         ticket,
         local_path,
         include_pattern,
         include_name,
         exclude_pattern,
         exclude_name):
    irods_commands.push(
        local_path=local_path,
        ticket=ticket,
        remote_path=remote_path,
        include_patterns=include_pattern,
        include_names=include_name,
        exclude_patterns=exclude_pattern,
        exclude_names=exclude_name)
