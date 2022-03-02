from os import environ
import requests


class TerrainToken:
    __access_token = None

    @staticmethod
    def get(username: str = None, password: str = None):
        if username is not None and password is not None:
            cyverse_username = username
            cyverse_password = password
        else:
            cyverse_username = environ.get('CYVERSE_USERNAME', None)
            cyverse_password = environ.get('CYVERSE_PASSWORD', None)

        if TerrainToken.__access_token is not None:
            return TerrainToken.__access_token

        if cyverse_username is None: raise ValueError("Missing environment variable 'CYVERSE_USERNAME'")
        if cyverse_password is None: raise ValueError("Missing environment variable 'CYVERSE_PASSWORD'")

        response = requests.get('https://de.cyverse.org/terrain/token/cas', auth=(cyverse_username, cyverse_password))
        response.raise_for_status()

        TerrainToken.__access_token = response.json()['access_token']

        return TerrainToken.__access_token
