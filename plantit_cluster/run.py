from dagster import usable_as_dagster_type


@usable_as_dagster_type
class Run(object):
    """
        PlantIT pipeline run definition.

        Attributes:
            workdir (str: The working directory.
            image (str): The Singularity or Docker container image (file or Singularity/Docker Hub URL).
            command (list): Command(s) to run inside the container.
            source (dict): Data store to pull from (optional).
            sink (dict): Data store to push to (optional).
    """

    def __init__(self,
                 workdir: str,
                 image: str,
                 command: list,
                 source: dict = None,
                 sink: dict = None):
        """
        Args:
            workdir (str: The pipeline working directory.
            image (str): The Singularity or Docker container image (file or Singularity/Docker Hub URL).
            command (list): Command(s) to run inside the container.
            source (dict): Data store to pull from (optional).
            sink (dict): Data store to push to (optional).
        """

        self._workdir = workdir
        self._image = image
        self._command = command
        self._source = source
        self._sink = sink

    @property
    def workdir(self):
        return self._workdir

    @property
    def image(self):
        return self._image

    @property
    def command(self):
        return self._command

    @property
    def source(self):
        return self._source

    @property
    def sink(self):
        return self._sink



