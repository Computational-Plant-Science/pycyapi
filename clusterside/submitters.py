import subprocess

'''
    Functions for submitting jobs scripts to different cluster types.
'''

def pbs(submit_script):
    '''
        Submit a job to a
        `PBS <https://en.wikipedia.org/wiki/Portable_Batch_System>`_
        job scheduler. This includes any scheduler that uses `qsub` for
        submissions

        Args:
            submit_script (str): path to submission script to submit.
    '''
    return subprocess.run(["qsub", submit_script],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

def slurm(submit_script):
    '''
        Submit a job to a
        `Slurm <https://www.schedmd.com/>`_
        job scheduler.

        Args:
            submit_script (str): path to submission script to submit.
    '''
    return subprocess.run(["sbatch", submit_script],
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
