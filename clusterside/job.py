class Job:
    """
        A single execution of a job.

        Attributes:
            id (str): The PlantIT job ID.
            workdir (str: The job working directory.
            token (str): The token to authenticate with the PlantIT web API.
            container (str): the Singularity Hub or Docker Hub container definition URL.
            commands (list): Commands to run inside the Singularity container.
            server (str): The PlantIT web API endpoint (optional).
            sources (dict): Workflow input sources (optional).
            targets (dict): Workflow output targets (optional).
    """

    def __init__(self,
                 id: str,
                 workdir: str,
                 token: str,
                 container: str,
                 commands: list,
                 server: str = None,
                 sources: dict = None,
                 targets: dict = None):
        """
        Args:
            id (str): The PlantIT job ID.
            workdir (str: The job working directory.
            token (str): The token to authenticate with the PlantIT web API.
            container (str): the Singularity Hub or Docker Hub container definition URL.
            commands (list): Commands to run inside the Singularity container.
            server (str): The PlantIT web API endpoint (optional).
            sources (dict): Workflow input sources (optional).
            targets (dict): Workflow output targets (optional).
        """

        self.id = id
        self.workdir = workdir
        self.token = token
        self.server = server
        self.container = container
        self.commands = commands
        self.sources = sources
        self.targets = targets
