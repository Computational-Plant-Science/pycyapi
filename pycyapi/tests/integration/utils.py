import time
import pprint
from os.path import basename
import requests

DEFAULT_SLEEP = 10


def list_files(token, path):
    with requests.get(f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?path={path}&limit=1000",
                      headers={'Authorization': 'Bearer ' + token}) as response:
        content = response.json()
        pprint.pprint(content)
        response.raise_for_status()
        return [f['label'] for f in content['files']]


def create_collection(token, path, sleep=DEFAULT_SLEEP):
    with requests.post('https://de.cyverse.org/terrain/secured/filesystem/directory/create',
                       json={'path': path},
                       headers={'Authorization': 'Bearer ' + token}) as response:
        pprint.pprint(response.json())
        response.raise_for_status()
    time.sleep(sleep)


def upload_file(token, local_path, remote_path, sleep=DEFAULT_SLEEP):
    with open(local_path, 'rb') as file:
        with requests.post(f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={remote_path}",
                           headers={'Authorization': f"Bearer {token}"},
                           files={'file': (basename(local_path), file, 'application/octet-stream')}) as response:
            pprint.pprint(response.json())
            response.raise_for_status()
    time.sleep(sleep * 3)


def delete_collection(token, path, sleep=DEFAULT_SLEEP):
    with requests.post('https://de.cyverse.org/terrain/secured/filesystem/delete',
                       json={'paths': [path]},
                       headers={'Authorization': 'Bearer ' + token}) as response:
        pprint.pprint(response.json())
        response.raise_for_status()
    time.sleep(sleep)
