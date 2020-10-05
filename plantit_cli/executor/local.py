import traceback

from dask.distributed import Client

from plantit_cli.executor.executor import Executor
from plantit_cli.run import Run
from plantit_cli.utils import update_status, execute_workflow_with_directory_input, execute_workflow_with_file_input, \
    execute_workflow_with_no_input


class InProcessExecutor(Executor):
    name = "local"

    def __init__(self, token: str):
        self.token = token

    def execute(self, run: Run):
        update_status(run, 3, f"Starting run '{run.identifier}' with '{self.name}' executor")
        try:
            with Client() as client:
                if run.clone is not None and run.clone is not '':
                    self.clone_repo(run)

                if run.input:
                    print(f"Pulling inputs for run '{run.identifier}'")
                    input_directory = self.pull_input(run)
                    if run.input['kind'].lower() == 'directory':
                        execute_workflow_with_directory_input(run, client, input_directory)
                    elif run.input['kind'].lower() == 'file':
                        execute_workflow_with_file_input(run, client, input_directory)
                    else:
                        raise ValueError(f"Value of 'input.kind' must be either 'file' or 'directory'")
                else:
                    print(f"Executing flow for run '{run.identifier}'")
                    execute_workflow_with_no_input(run, client)

                if run.output:
                    print(f"Pushing outputs for run '{run.identifier}'")
                    self.push_output(run)
        except Exception:
            update_status(run, 3, f"Run '{run.identifier}' failed: {traceback.format_exc()}")
            return
