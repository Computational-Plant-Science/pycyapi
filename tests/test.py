from clusterside.executors import SingleJobExecutor
from clusterside.data import Collection, Workflow
from clusterside.comms import STDOUTComms

from process import WORKFLOW_CONFIG

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
	"auth_token": "asdf",
	"job_pk": 2,
	"parameters": {}
}
'''

with open("samples.json", "w") as fout:
    fout.write(sample_file)

with open("workflow.json", "w") as fout:
    fout.write(workflow_file)

collection = Collection("samples.json")
workflow = Workflow(WORKFLOW_CONFIG,"workflow.json")
server = STDOUTComms()

executor = SingleJobExecutor(collection, workflow, server)
executor.process()
executor.reduce()
