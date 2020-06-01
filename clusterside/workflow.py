class Workflow:
    """
        Contains information about the workflow to be run

        Attributes:
            job_pk (int): The PlantIT job ID.
            token (str): The token to authenticate with the PlantIT web API.
            server_url (str): The PlantIT web API endpoint.
            container_url (str): the Singularity Hub or Docker Hub container definition URL.
            pre_commands (str): Optional commands to run before starting the workflow.
            post_commands (str): Optional commands to run after the workflow completes.
            commands (str): Commands to run inside the Singularity container.
            flags (list): Optional flags to add to `singularity exec` when running the Singularity container. Should be a list
                in the format used by subprocess.run.
    """

    def __init__(self,
                 job_pk: str,
                 token: str,
                 server_url: str,
                 container_url: str,
                 commands: str,
                 pre_commands: str = None,
                 post_commands: str = None,
                 flags: list = []):
        """
        Args:
            job_pk (int): The PlantIT job ID.
            token (str): The token to authenticate with the PlantIT web API.
            server_url (str): The PlantIT web API endpoint.
            container_url (str): the Singularity Hub or Docker Hub container definition URL.
            pre_commands (str): Optional commands to run before starting the workflow.
            post_commands (str): Optional commands to run after the workflow completes.
            commands (str): Commands to run inside the Singularity container.
            flags (list): Optional flags to add to `singularity exec` when running the Singularity container. Should be a list
                in the format used by subprocess.run.
        """

        self.job_pk = job_pk
        self.token = token
        self.server_url = server_url
        self.container_url = container_url
        self.pre_commands = pre_commands
        self.post_commands = post_commands
        self.commands = commands
        self.flags = flags