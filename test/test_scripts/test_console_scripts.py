import subprocess
from importlib.metadata import entry_points
import pytest


def get_openmsistream_console_scripts():
    return [
        ep
        for ep in entry_points().get("console_scripts", [])
        if ep.value.startswith("openmsistream.")
    ]


@pytest.mark.parametrize(
    "script",
    get_openmsistream_console_scripts(),
    ids=lambda s: s.name,
)
def test_console_scripts_exist(script):
    """
    Ensure each console script for openmsistream exists and responds to --help.
    """
    name = script.name

    try:
        subprocess.check_output([name, "--help"], stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as error:
        errmsg = (
            f'ERROR: test for console script "{name}" failed with output:\n'
            f"{error.output.decode()}"
        )
        raise RuntimeError(errmsg)
