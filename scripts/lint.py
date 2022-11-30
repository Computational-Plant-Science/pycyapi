import os
from pathlib import Path

try:
    import isort

    print(f"isort version: {isort.__version__}")
except ModuleNotFoundError:
    print("isort not installed\n\tInstall using pip install isort")

try:
    import black

    print(f"black version: {black.__version__}")
except ModuleNotFoundError:
    print("black not installed\n\tInstall using pip install black")

project_root_path = Path(__file__).parent.parent
source_path = project_root_path / "pycyapi"

print("running isort...")
os.system(f"isort -v {source_path}")

print("running black...")
os.system(f"black -v {source_path}")