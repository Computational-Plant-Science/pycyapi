import os
from os import listdir, remove
from os.path import isdir, isfile, islink, join
import json
import shutil
import requests


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


class TerrainTicket:
    @staticmethod
    def get(path: str, mode: str = 'read', public: bool = True, uses: int = 1):
        response = requests.post(
            f"https://de.cyverse.org/terrain/secured/filesystem/tickets?mode={mode}&public={str(public).lower()}&uses-limit={uses}",
            data=json.dumps({'paths': [path]}),
            headers={'Authorization': f"Bearer {TerrainToken.get()}", 'Content-Type': 'application/json'}).json()
        # print(response)
        return response['tickets'][0]['ticket-id']


class TerrainToken:
    __token = None

    @staticmethod
    def get():
        if TerrainToken.__token is not None:
            return TerrainToken.__token

        cyverse_username = os.environ.get('CYVERSE_USERNAME', None)
        cyverse_password = os.environ.get('CYVERSE_PASSWORD', None)

        if cyverse_username is None: raise ValueError("Missing environment variable 'CYVERSE_USERNAME'")
        if cyverse_password is None: raise ValueError("Missing environment variable 'CYVERSE_PASSWORD'")

        response = requests.get('https://de.cyverse.org/terrain/token/cas', auth=(cyverse_username, cyverse_password)).json()
        # print(response)
        TerrainToken.__token = response['access_token']
        return TerrainToken.__token