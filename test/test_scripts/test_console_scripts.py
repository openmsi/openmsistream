# imports
import pkg_resources, subprocess, unittest  # pylint: disable=wrong-import-order


class TestConsoleScripts(unittest.TestCase):
    """
    Make sure console scripts are defined and can be imported
    """

    def test_console_scripts_exist(self):
        """
        Make sure console scripts defined in setup.py exist and that their imports work
        """
        for script in pkg_resources.iter_entry_points("console_scripts"):
            if script.dist.key == "openmsistream":
                with self.subTest(script=script.name):
                    try:
                        subprocess.check_output(
                            [script.name, "--help"], stderr=subprocess.STDOUT
                        )
                    except subprocess.CalledProcessError as error:
                        errmsg = (
                            f'ERROR: test for console script "{script.name}" '
                            f"failed with output:\n{error.output.decode()}"
                        )
                        raise RuntimeError(errmsg)
