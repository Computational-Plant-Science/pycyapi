from dagster import usable_as_dagster_type

from plantit_cluster.input.input import Input


@usable_as_dagster_type
class Pipeline(object):
    """
        PlantIT pipeline definition.

        Attributes:
            workdir (str: The working directory.
            image (str): The Singularity Hub or Docker Hub image URL.
            commands (list): Commands to run inside the container.
            params (list): Parameters to pass as environment variables to the container.
            input (Input): Data store to pull inputs from (optional).
    """

    def __init__(self,
                 workdir: str,
                 image: str,
                 commands: list,
                 params: list = [],
                 input: Input = None):
        """
        Args:
            workdir (str: The pipeline working directory.
            image (str): The Singularity Hub or Docker Hub image URL.
            commands (list): Commands to run inside the container.
            params (list): Parameters to pass as environment variables to the container.
            input (Input): Data store to pull inputs from (optional).
        """

        self._workdir = workdir
        self._image = image
        self._commands = commands
        self._params = params
        self._input = input

    @property
    def workdir(self):
        return self._workdir

    @property
    def image(self):
        return self._image

    @property
    def commands(self):
        return self._commands

    @property
    def params(self):
        return self._params

    @property
    def input(self):
        return self._input



