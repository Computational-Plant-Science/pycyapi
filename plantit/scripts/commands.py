from pathlib import Path

from plantit.scripts.generator import ScriptGenerator
from plantit.scripts.models import SubmissionConfig


def scripts(config: SubmissionConfig):
    generator = ScriptGenerator(config)
    scripts = dict()

    if config.source:
        scripts[f"pull.{config.guid}.sh"] = generator.gen_pull_script()

    scripts[f"run.{config.guid}.sh"] = generator.gen_job_script()

    if config.sink:
        scripts[f"push.{config.guid}.sh"] = generator.gen_push_script()

    for name, script in scripts.items():
        with open(name, 'wt', encoding='utf-8') as f:
            for line in script:
                f.write(line)

    return [Path(name) for name in scripts.keys()]

