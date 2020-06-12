from dagster.core.definitions import repository

from .pipelines import *


@repository
def plantit_repository():
    return [docker, singularity]
