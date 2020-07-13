from dagster import usable_as_dagster_type


@usable_as_dagster_type
class Run(object):
    def __init__(self,
                 identifier: str,
                 api_url: str,
                 workdir: str,
                 image: str,
                 command: str = None,
                 clone: str = None,
                 token: str = None,
                 params: list = None,
                 input: dict = None,
                 output: dict = None):
        self._identifier = identifier
        self._api_url = api_url
        self._workdir = workdir
        self._image = image
        self._command = command
        self._clone = clone
        self._token = token
        self._params = params
        self._input = input
        self._output = output

    @property
    def identifier(self):
        return self._identifier

    @property
    def api_url(self):
        return self._api_url

    @property
    def workdir(self):
        return self._workdir

    @property
    def clone(self):
        return self._clone

    @property
    def image(self):
        return self._image

    @property
    def command(self):
        return self._command

    @property
    def token(self):
        return self._token

    @property
    def params(self):
        return self._params

    @property
    def input(self):
        return self._input

    @property
    def output(self):
        return self._output



