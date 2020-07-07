import json

import click

from plantit_cluster.executor.inprocessexecutor import InProcessExecutor
from plantit_cluster.executor.jobqueueexecutor import JobQueueExecutor
from plantit_cluster.input.irodsinput import IRODSInput
from plantit_cluster.pipeline import Pipeline


@click.group()
def cli():
    pass


def object_hook(dct):
    if 'path' in dct:
        print(dct)
        return IRODSInput(**dct)
    return dct


@cli.command()
@click.argument('path')
def run(path):
    with open(path) as file:
        definition = json.load(file, object_hook=object_hook)

        if 'in-process' in definition['executor']['name']:
            executor = InProcessExecutor()
        elif 'pbs' in definition['executor']['name']:
            executor = JobQueueExecutor(**definition['executor'])
        elif 'slurm' in definition['executor']['name']:
            executor = JobQueueExecutor(**definition['executor'])
        else:
            raise ValueError(f"Unrecognized executor (supported: 'in-process', 'pbs', 'slurm')")

        del definition['executor']
        pipeline = Pipeline(**definition)
        print(pipeline.__dict__)
        executor.execute(pipeline)


if __name__ == '__main__':
    cli()

# def cli():
#    parser = argparse.ArgumentParser(description="PlantIT workflow management API.")
#    _, args = parser.parse_known_args(sys.argv[1:])
#    parser.add_argument('--pipeline',
#                        type=str,
#                        help="JSON pipeline definition file")
#    opts = parser.parse_args(args)
#
#    with open(opts.pipeline) as file:
#        pipeline = Pipeline(**json.load(file, object_hook=job_hook))
#
#    pipeline.executor.execute(pipeline)
#
#
# if __name__ == "__main__":
#    cli()
