# conftest.py
import sys


# Mock the process.py module usually provided by PlantIT before calling
# clusterside. Required to run test_bootstrapper.py
def process_sample(name, path, args):
    from pathlib import Path
    Path('a_really_awecsome_file.txt').touch()
    return {
        'key-val': {'name': name, 'path': path}
    }


module = type(sys)('process')
module.process_sample = process_sample
sys.modules['process'] = module
