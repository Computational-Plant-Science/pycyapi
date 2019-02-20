from clusterside.executors import SingleJobExecutor

sample_file = '''
{
	"Sample1": {
		"storage": "irods",
		"path": "/iplant/home/cotter/DSC_1014.JPG"
	},
	"Sample2": {
		"storage": "irods",
		"path": "/iplant/home/cotter/DSC_1015.JPG"
	},
	"Sample3": {
		"storage": "irods",
		"path": "/iplant/home/cotter/DSC_1016.JPG"
	}
}
'''

workflow_file = '''
{
    "singularity_url": "shub://frederic-michaud/python3"
}
'''

with open("samples.json", "w") as fout:
    fout.write(sample_file)

with open("workflow.json", "w") as fout:
    fout.write(workflow_file)

executor = SingleJobExecutor()
executor.process()
