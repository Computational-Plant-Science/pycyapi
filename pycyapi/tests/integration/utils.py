import time
import json
import pprint
from os.path import basename
import requests

DEFAULT_SLEEP = 10


def stat_file(token, path, print_response=False):
    response = requests.post(
        "https://de.cyverse.org/terrain/secured/filesystem/stat",
        data=json.dumps({'paths': [path]}),
        headers={'Authorization': f"Bearer {token}",
                 "Content-Type": 'application/json;charset=utf-8'})

    if response.status_code == 500 and response.json()['error_code'] == 'ERR_DOES_NOT_EXIST':
        raise ValueError(f"Path {path} does not exist")
    elif response.status_code == 400:
        raise ValueError(f"Bad request: {response}")

    response.raise_for_status()
    content = response.json()
    if print_response: pprint.pprint(content)

    paths = content.get('paths', None)
    if paths is None: raise ValueError(f"No paths on response: {content}")
    info = paths.get(path, None)
    if info is None: raise ValueError(f"No path info on response: {content}")
    return info


def get_metadata(token, id, print_response=False):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json;charset=utf-8"
    }
    response = requests.get(f"https://de.cyverse.org/terrain/secured/filesystem/{id}/metadata", headers=headers)
    response.raise_for_status()
    content = response.json()

    if print_response: pprint.pprint(content)

    avus = content['avus']
    return avus


def set_metadata(token, id, attributes, print_response=False):
    def to_avu(attr: str):
        split = attr.strip().split('=')
        return {
            'attr': split[0],
            'value': split[1],
            'unit': ''
        }

    data = {'avus': [to_avu(a) for a in attributes], 'irods-avus': [], }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json;charset=utf-8"
    }
    response = requests.post(f"https://de.cyverse.org/terrain/secured/filesystem/{id}/metadata",
                             data=json.dumps(data),
                             headers=headers)
    response.raise_for_status()
    content = response.json()
    if print_response: pprint.pprint(content)


def list_files(token, path, print_response=False):
    with requests.get(f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?path={path}&limit=1000",
                      headers={'Authorization': 'Bearer ' + token}) as response:
        content = response.json()
        if print_response: pprint.pprint(content)
        response.raise_for_status()
        return [f['label'] for f in content['files']]


def list_directories(token, path, print_response=False):
    with requests.get(f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?path={path}&limit=1000",
                      headers={'Authorization': 'Bearer ' + token}) as response:
        content = response.json()
        if print_response: pprint.pprint(content)
        response.raise_for_status()
        return [f['label'] for f in content['folders']]


def create_collection(token, path, sleep=DEFAULT_SLEEP, print_response=False):
    with requests.post('https://de.cyverse.org/terrain/secured/filesystem/directory/create',
                       json={'path': path},
                       headers={'Authorization': 'Bearer ' + token}) as response:
        if print_response: pprint.pprint(response.json())
        response.raise_for_status()
    time.sleep(sleep)


def upload_file(token, local_path, remote_path, sleep=DEFAULT_SLEEP, print_response=False):
    with open(local_path, 'rb') as file:
        with requests.post(f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={remote_path}",
                           headers={'Authorization': f"Bearer {token}"},
                           files={'file': (basename(local_path), file, 'application/octet-stream')}) as response:
            if print_response: pprint.pprint(response.json())
            response.raise_for_status()
    time.sleep(sleep * 3)


def delete_collection(token, path, sleep=DEFAULT_SLEEP, print_response=False):
    with requests.post('https://de.cyverse.org/terrain/secured/filesystem/delete',
                       json={'paths': [path]},
                       headers={'Authorization': 'Bearer ' + token}) as response:
        if print_response: pprint.pprint(response.json())
        response.raise_for_status()
    time.sleep(sleep)
