import os
import subprocess
import traceback
from abc import ABC
from os.path import join, isdir

from plantit_cli.exceptions import PlantitException
from plantit_cli.plan import Plan
from plantit_cli.store.store import Store
from plantit_cli.utils import update_status, run_container_for_directory, run_containers_for_files, \
    run_container


class Runner(ABC):
    def __init__(self, store: Store):
        self.__store = store

    @staticmethod
    def __clone_repo(plan: Plan):
        repo_dir = plan.clone.rpartition('/')[2]
        if isdir(repo_dir):
            if subprocess.run(f"cd {repo_dir} && git pull",
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              shell=True).returncode != 0:
                raise PlantitException(f"Repo '{plan.clone}' exists on local filesystem, failed to pull updates")
            else:
                update_status(plan, 3, f"Updated repo '{plan.clone}'")
        elif subprocess.run(f"git clone {plan.clone}",
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True).returncode != 0:
            raise PlantitException(f"Failed to clone repo '{plan.clone}'")
        else:
            update_status(plan, 3, f"Cloned repo '{plan.clone}'")

    def __pull_input(self, plan: Plan) -> str:
        input_dir = join(plan.workdir, 'input')
        input_kind = plan.input['kind'].lower()
        os.makedirs(input_dir, exist_ok=True)
        if input_kind == 'directory' or input_kind == 'files':
            self.__store.download_directory(plan.input['from'], input_dir,
                                            plan.input['pattern'] if 'pattern' in plan.input else None)
        elif input_kind == 'file':
            self.__store.download_file(plan.input['from'], input_dir)
        else:
            raise ValueError(f"'input.kind' must be either 'file' or 'directory'")
        input_files = os.listdir(input_dir)
        if len(input_files) == 0:
            raise PlantitException(f"No inputs found at path '{plan.input['from']}'" + (
                f" matching pattern '{plan.input['pattern']}'" if 'pattern' in plan.input else ''))
        update_status(plan, 3, f"Pulled input(s): {', '.join(input_files)}")
        return input_dir

    def __push_output(self, plan: Plan):
        self.__store.upload_directory(
            join(plan.workdir, plan.output['from']) if 'from' in plan.output else plan.workdir,
            plan.output['to'],
            (plan.output['include']['patterns'] if type(plan.output['include']['patterns']) is list else None) if 'include' in plan.output else None,
            (plan.output['include']['names'] if type(plan.output['include']['names']) is list else None) if 'include' in plan.output else None,
            (plan.output['exclude']['patterns'] if type(plan.output['exclude']['patterns']) is list else None) if 'exclude' in plan.output else None,
            (plan.output['exclude']['names'] if type(plan.output['exclude']['names']) is list else None) if 'exclude' in plan.output else None)

        update_status(plan, 3, f"Pushed output(s)")

    def run(self, plan: Plan):
        update_status(plan, 3, f"Starting '{plan.identifier}'")

        try:
            if plan.clone is not None and plan.clone != '':
                Runner.__clone_repo(plan)

            if plan.input:
                print(f"Pulling input(s)")
                input_dir = self.__pull_input(plan)
                input_kind = plan.input['kind'].lower()
                if input_kind == 'directory':
                    run_container_for_directory(plan, input_dir)
                elif input_kind == 'files' or input_kind == 'file':
                    run_containers_for_files(plan, input_dir)
                else:
                    raise ValueError(f"'input.kind' must be 'file', 'files', or 'directory'")
            else:
                run_container(plan)

            if plan.output:
                print(f"Pushing output(s)")
                self.__push_output(plan)
        except Exception as e:
            update_status(plan, 2, f"'{plan.identifier}' failed: {traceback.format_exc()}")
            raise e

        update_status(plan, 1, f"Completed '{plan.identifier}'")
