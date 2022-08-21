from os import environ

import requests


class AccessToken:
    """
    Singleton for caching access tokens in-memory for the duration of a session.
    Attempts to read CyVerse username/password from environment variables if not explicitly provided.
    """

    __access_token = None

    @staticmethod
    def get(username: str = None, password: str = None):
        """
        Retrieve a token, either from the cache or via a new request to Terrain.

        :param username: The CyVerse username
        :param password: The CyVerse password
        :return: The access token
        """

        # TODO: automatic refresh of tokens near-expiry and refusal to return expired tokens

        if username is not None and password is not None:
            cyverse_username = username
            cyverse_password = password
        else:
            cyverse_username = environ.get('CYVERSE_USERNAME', None)
            cyverse_password = environ.get('CYVERSE_PASSWORD', None)

        if AccessToken.__access_token is not None:
            return AccessToken.__access_token

        if cyverse_username is None: raise ValueError("Missing environment variable 'CYVERSE_USERNAME'")
        if cyverse_password is None: raise ValueError("Missing environment variable 'CYVERSE_PASSWORD'")

        response = requests.get('https://de.cyverse.org/terrain/token/cas', auth=(cyverse_username, cyverse_password))
        response.raise_for_status()

        AccessToken.__access_token = response.json()['access_token']

        return AccessToken.__access_token
