from os import environ as env

from pycyapi.clients import TerrainClient
import pycyapi.tests.integration.utils as testutils
from pycyapi.auth import AccessToken


token = AccessToken.get()
client = TerrainClient(token)
