import click

from plantit.cli import cli
import plantit.scripts.commands as commands


@cli.command()
def scripts():
    click.echo('not implemented yet')
