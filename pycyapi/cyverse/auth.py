import json
from os import environ
from pathlib import Path
from tempfile import gettempdir
from typing import List

import requests
from filelock import FileLock


class CyverseAccessToken:
    """
    Singleton for caching CyVerse access tokens in a thread-safe manner for the duration of a session.
    Attempts to read CyVerse username/password from environment variables if not explicitly provided.
    """

    __token = None
    __lock = FileLock(Path(gettempdir()) / f"pycyapi_token.lock")

    @staticmethod
    def get(username: str = None, password: str = None):
        """
        Retrieve a token, either from the cache or via a new request to Terrain.

        Parameters
        :param username: The CyVerse username
        :param password: The CyVerse password
        :return: The access token
        """

        # TODO: automatic refresh of tokens near-expiry
        #  also don't return expired tokens

        if username is not None and password is not None:
            cyverse_username = username
            cyverse_password = password
        else:
            cyverse_username = environ.get("CYVERSE_USERNAME", None)
            cyverse_password = environ.get("CYVERSE_PASSWORD", None)

        CyverseAccessToken.__lock.acquire()
        try:
            if CyverseAccessToken.__token is not None:
                return CyverseAccessToken.__token

            if cyverse_username is None:
                raise ValueError(
                    "Missing environment variable 'CYVERSE_USERNAME'"
                )
            if cyverse_password is None:
                raise ValueError(
                    "Missing environment variable 'CYVERSE_PASSWORD'"
                )

            response = requests.get(
                "https://de.cyverse.org/terrain/token/cas",
                auth=(cyverse_username, cyverse_password),
            )
            response.raise_for_status()

            CyverseAccessToken.__token = response.json()["access_token"]
            return CyverseAccessToken.__token
        finally:
            CyverseAccessToken.__lock.release()


class CyverseFilesystemTicket:
    @staticmethod
    def get(
        paths: List[str],
        mode: str = "read",
        public: bool = False,
        uses: int = 10,
    ):
        response = requests.post(
            f"https://de.cyverse.org/terrain/secured/filesystem/tickets"
            f"?mode={mode}"
            f"&public={str(public).lower()}"
            f"&uses-limit={uses}",
            data=json.dumps({"paths": paths}),
            headers={
                "Authorization": f"Bearer {CyverseAccessToken.get()}",
                "Content-Type": "application/json",
            },
        ).json()
        return response["tickets"][0]["ticket-id"]
