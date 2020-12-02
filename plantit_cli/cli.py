import click
import yaml

from plantit_cli.runner.runner import Runner
from plantit_cli.plan import Plan
from plantit_cli.store.terrain_store import TerrainStore


@click.command()
@click.argument('workflow')
@click.option('--plantit_token', required=False, type=str)
@click.option('--cyverse_token', required=False, type=str)
def run(workflow, plantit_token, cyverse_token):
    with open(workflow, 'r') as file:
        workflow_def = yaml.safe_load(file)
        workflow_def['plantit_token'] = plantit_token
        workflow_def['cyverse_token'] = cyverse_token

        if 'api_url' not in workflow_def:
            workflow_def['api_url'] = None

        Runner(TerrainStore(cyverse_token)).run(Plan(**workflow_def))
