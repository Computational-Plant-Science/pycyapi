from clusterside.executors import SingleJobExecutor
from clusterside.data import Collection, Workflow
from clusterside.comms import STDOUTComms

process_file = '''
def process_sample(name,path,args):
    from pathlib import Path
    Path('a_really_awecsome_file.txt').touch()
    return {
        'key-val': {'name': name, 'path': path}
    }
'''

sample_file = '''
{
	"Sample1": {
		"storage": "irods",
		"path": "/tempZone/home/rods/DSC_0002.JPG"
	},
	"Sample2": {
		"storage": "irods",
		"path": "/tempZone/home/rods/DSC_0003.JPG"
	},
	"Sample3": {
		"storage": "irods",
		"path": "/tempZone/home/rods/DSC_0004.JPG"
	}
}
'''

workflow_file = '''
{
	"server_url": "http://localhost/jobs/api/",
    "singularity_url": "shub://frederic-michaud/python3",
    "api_version": 0.1,
	"auth_token": "asdf",
	"job_pk": 2,
	"parameters": {}
}
'''

with open("samples.json", "w") as fout:
    fout.write(sample_file)

with open("workflow.json", "w") as fout:
    fout.write(workflow_file)

with open("process.py", "w") as fout:
    fout.write(process_file)

collection = Collection("samples.json")
workflow = Workflow("workflow.json")
server = STDOUTComms()

executor = SingleJobExecutor(collection, workflow, server)
executor.process()
results_path = executor.reduce()
server.update_job({'remote_results_path': results_path})
server.task_complete(1)
