import time
import requests

DEFAULT_SLEEP = 45


def create_collection(path, token, sleep=DEFAULT_SLEEP):
    time.sleep(sleep)
    print(requests.post('https://de.cyverse.org/terrain/secured/filesystem/directory/create',
                        json={'path': path},
                        headers={'Authorization': 'Bearer ' + token}).json())
    time.sleep(sleep)


def list_files(path, token, sleep=DEFAULT_SLEEP):
    time.sleep(sleep)
    response = requests.get(
        f"https://de.cyverse.org/terrain/secured/filesystem/paged-directory?path={path}&limit=1000",
        headers={'Authorization': 'Bearer ' + token})
    content = response.json()
    print(content)
    return content['files']


def upload_file(local_path, remote_path, token, sleep=DEFAULT_SLEEP):
    print(requests.post(
        f"https://de.cyverse.org/terrain/secured/fileio/upload?dest={remote_path}",
        headers={'Authorization': 'Bearer ' + token},
        files={'file': open(local_path, 'r')}).json())
    time.sleep(sleep)


def delete_collection(path, token, sleep=DEFAULT_SLEEP):
    print(requests.post('https://de.cyverse.org/terrain/secured/filesystem/delete',
                        json={'paths': [path]},
                        headers={'Authorization': 'Bearer ' + token}).json())
    time.sleep(sleep)
