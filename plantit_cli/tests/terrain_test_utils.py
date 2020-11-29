import time
import pprint
from os.path import basename
import requests

DEFAULT_SLEEP = 50


def create_collection(path, token, sleep=DEFAULT_SLEEP):
    time.sleep(sleep)
    with requests.post('https://de.cyverse.org/terrain/secured/filesystem/directory/create',
                       json={'path': path},
                       headers={'Authorization': 'Bearer ' + token}) as response:
        pprint.pprint(response.json())
        response.raise_for_status()
    time.sleep(sleep)


def list_files(path, token, sleep=DEFAULT_SLEEP):
    time.sleep(sleep)
    with requests.get(f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?path={path}&limit=1000",
                      headers={'Authorization': 'Bearer ' + token}) as response:
        content = response.json()
        pprint.pprint(content)
        response.raise_for_status()
        return content['files']


def upload_file(local_path, remote_path, token, sleep=DEFAULT_SLEEP):
    with open(local_path, 'rb') as file:
        with requests.post(f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={remote_path}",
                           headers={'Authorization': f"Bearer {token}"},
                           files={'file': (basename(local_path), file, 'application/octet-stream')}) as response:
            pprint.pprint(response.json())
            response.raise_for_status()
    time.sleep(sleep)


def delete_collection(path, token, sleep=DEFAULT_SLEEP):
    with requests.post('https://de.cyverse.org/terrain/secured/filesystem/delete',
                       json={'paths': [path]},
                       headers={'Authorization': 'Bearer ' + token}) as response:
        pprint.pprint(response.json())
        response.raise_for_status()
    time.sleep(sleep)
