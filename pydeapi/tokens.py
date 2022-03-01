import os
import requests


class TerrainToken:
    __token = None

    @staticmethod
    def get(username, password):
        if username is not None and password is not None:
            cyverse_username = username
            cyverse_password = password
        else:
            cyverse_username = os.environ.get('CYVERSE_USERNAME', None)
            cyverse_password = os.environ.get('CYVERSE_PASSWORD', None)

        if TerrainToken.__token is not None:
            return TerrainToken.__token

        if cyverse_username is None: raise ValueError("Missing environment variable 'CYVERSE_USERNAME'")
        if cyverse_password is None: raise ValueError("Missing environment variable 'CYVERSE_PASSWORD'")

        response = requests.get('https://de.cyverse.org/terrain/token/cas', auth=(cyverse_username, cyverse_password))
        response.raise_for_status()

        TerrainToken.__token = response.json()['access_token']
        return TerrainToken.__token
