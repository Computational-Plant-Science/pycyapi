import uuid
from os import environ
from os.path import join, isdir

import pytest
from irods.session import iRODSSession
from irods.ticket import Ticket
from irods.exception import CollectionDoesNotExist

from plantit_cli.store import irods_commands
from plantit_cli.tests.utils import clear_dir, TerrainToken, TerrainTicket

message = "Message!"
testdir = environ.get('TEST_DIRECTORY')


# def test_pull_when_path_does_not_exist(remote_base_path):
#     remote_path = join(remote_base_path, str(uuid.uuid4()))
#     ticket = TerrainTicket.get(remote_path)
#     with pytest.raises(CollectionDoesNotExist):
#         irods_commands.pull(remote_path, testdir, ticket=ticket)
#
#
# def test_pull_when_ticket_invalid(remote_base_path):
#     remote_path = join(remote_base_path, str(uuid.uuid4()))
#     ticket = TerrainTicket.get(remote_path)
#     with pytest.raises(CollectionDoesNotExist):
#         irods_commands.push(testdir, remote_path, ticket=ticket)