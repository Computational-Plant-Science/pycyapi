import re
import traceback


def clean_html(raw_html: str) -> str:
    expr = re.compile("<.*?>")
    text = re.sub(expr, "", raw_html)
    return text


def parse_job_id(line: str) -> str:
    try:
        return str(int(line.replace("Submitted batch job", "").strip()))
    except:
        raise Exception(
            f"Failed to parse job ID from '{line}'\n{traceback.format_exc()}"
        )
