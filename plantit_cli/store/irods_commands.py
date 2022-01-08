from irods.session import iRODSSession
from irods.models import Collection
from irods.ticket import Ticket


def pull(ticket: str):
    # user = 'anonymous', password = ''
    session = iRODSSession(host='data.cyverse.org', port=1247, user='anonymous', password='', zone='iplant')
    Ticket(session, ticket).supply()

    # TODO invoke store, e.g.
    # irods_store.pull_dir()


def push():
    pass