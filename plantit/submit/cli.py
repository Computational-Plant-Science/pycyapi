import click

from plantit.cli import cli
import plantit.submit.commands as commands


@cli.command()
def submit():
    click.echo('not implemented yet')
