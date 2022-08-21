from os import listdir, remove
from os.path import isdir, isfile, islink, join
import json
import shutil
from typing import List

import requests

from pycyapi.auth import AccessToken


def clear_dir(dir):
    for file in listdir(dir):
        p = join(dir, file)
        if isfile(p) or islink(p):
            remove(p)
        elif isdir(p):
            shutil.rmtree(p)


def check_hello(file, name):
    assert isfile(file)
    with open(file) as file:
        lines = file.readlines()
        assert len(lines) == 1
        line = lines[0]
        assert f"Hello, {name}!" in line


# TODO: use once write ticket granting issue is fixed (https://github.com/irods/irods/issues/5913)
class TerrainTicket:
    @staticmethod
    def get(paths: List[str], mode: str = 'read', public: bool = False, uses: int = 10):
        response = requests.post(
            f"https://de.cyverse.org/terrain/secured/filesystem/tickets?mode={mode}&public={str(public).lower()}&uses-limit={uses}",
            data=json.dumps({'paths': paths}),
            headers={'Authorization': f"Bearer {AccessToken.get()}", 'Content-Type': 'application/json'}).json()
        return response['tickets'][0]['ticket-id']
