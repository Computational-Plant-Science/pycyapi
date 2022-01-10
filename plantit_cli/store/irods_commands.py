import os
import ssl
import logging
import traceback
from pathlib import Path
from os import getcwd, listdir
from os.path import isfile, expanduser
from typing import List

from irods.session import iRODSSession
from irods.ticket import Ticket

from plantit_cli.store import irods_store

logger = logging.getLogger(__name__)


def create_session(ticket: str = None):
    # if we have a ticket, create an anonymous session and apply it to it
    if ticket is not None and ticket != '':
        session = iRODSSession(host='data.cyverse.org', port=1247, user='anonymous', password='', zone='iplant')
        Ticket(session, ticket).supply()
        return session

    # otherwise check for an iRODS environment file (if we don't have one, abort)
    try:
        env_file = os.environ.get('IRODS_ENVIRONMENT_FILE')
    except KeyError:
        default_env_file = '~/.irods/irods_environment.json'
        if isfile(default_env_file): env_file = expanduser(default_env_file)
        else: raise ValueError(f"No iRODS authentication method provided (need ticket or environment file)")

    ssl_context = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH, cafile=None, capath=None, cadata=None)
    ssl_settings = {'ssl_context': ssl_context}
    return iRODSSession(irods_env_file=env_file, **ssl_settings)


def pull(
        remote_path: str,
        local_path: str = None,
        ticket: str = None,
        patterns: List[str] = None,
        checksums: List[dict] = None,
        overwrite: bool = False):
    try:
        # make sure local path exists
        local_path = getcwd() if (local_path is None or local_path == '') else local_path
        Path(local_path).mkdir(exist_ok=True)

        # create session
        session = create_session(ticket)

        # if directory, pull all files matching patterns
        if irods_store.dir_exists(remote_path, session):
            irods_store.pull_dir(from_path=remote_path,
                                 to_path=local_path,
                                 patterns=patterns,
                                 checksums=checksums,
                                 session=session,
                                 overwrite=overwrite)
        # if single file, pull it
        elif irods_store.file_exists(remote_path, session):
            irods_store.pull_file(
                from_path=remote_path,
                to_path=local_path,
                session=session,
                overwrite=overwrite)
        # otherwise the given path doesn't exist
        else:
            msg = f"Path does not exist: {remote_path}"
            logger.error(msg)
            raise ValueError(msg)

        # make sure we actually downloaded anything
        files = listdir(local_path)
        if len(files) == 0:
            msg = f"No files found at path '{remote_path}'" + f" matching patterns {patterns}"
            logger.error(msg)
            raise ValueError(msg)

        logger.info(f"Pulled input(s): {', '.join(files)}")
        return local_path
    except:
        logger.error(f"Pull failed: {traceback.format_exc()}")
        raise


def push(
        local_path: str,
        remote_path: str,
        ticket: str = None,
        include_patterns: List[str] = None,
         include_names: List[str] = None,
         exclude_patterns: List[str] = None,
         exclude_names: List[str] = None):
    try:
        # create session
        session = create_session(ticket)

        # push the contents of the directory matching the given patterns
        irods_store.push_dir(
            from_path=local_path,
            to_prefix=remote_path,
            include_patterns=include_patterns,
            include_names=include_names,
            exclude_patterns=exclude_patterns,
            exclude_names=exclude_names,
            session=session)
        logger.info(f"Pushed output(s)")
    except:
        logger.error(f"Push failed: {traceback.format_exc()}")
        raise