from pathlib import Path

from plantit.scripts.generator import ScriptGenerator
from plantit.scripts.models import ScriptConfig


def scripts(config: ScriptConfig):
    generator = ScriptGenerator(config)
    files = dict()

    if config.source:
        files[f"pull.{config.guid}.sh"] = generator.gen_pull_script()

    files[f"run.{config.guid}.sh"] = generator.gen_job_script()

    if config.sink:
        files[f"push.{config.guid}.sh"] = generator.gen_push_script()

    for name, script in files.items():
        with open(name, 'wt', encoding='utf-8') as f:
            for line in script:
                f.write(line)

    return [Path(name) for name in files.keys()]

