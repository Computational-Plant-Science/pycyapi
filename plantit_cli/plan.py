class Plan(object):
    def __init__(self,
                 identifier: str,
                 workdir: str,
                 image: str,
                 mount: list = None,
                 api_url: str = None,
                 command: str = None,
                 clone: str = None,
                 plantit_token: str = None,
                 cyverse_token: str = None,
                 params: list = None,
                 input: dict = None,
                 output: dict = None):
        self.identifier = identifier
        self.workdir = workdir
        self.image = image
        self.mount = mount
        self.api_url = api_url
        self.command = command
        self.clone = clone
        self.plantit_token = plantit_token
        self.cyverse_token = cyverse_token
        self.params = params
        self.input = input
        self.output = output
