#imports
import pkg_resources, subprocess, unittest

class TestConsoleScripts(unittest.TestCase):
    
    def test_console_scripts_exist(self):
        """
        Make sure console scripts defined in setup.py exist and that their imports work
        """
        for script in pkg_resources.iter_entry_points('console_scripts') :
            if script.dist.key == 'openmsistream' :
                with self.subTest(script=script.name):
                    try:
                        subprocess.check_output([script.name, '--help'],stderr=subprocess.STDOUT)
                    except subprocess.CalledProcessError as e:
                        errmsg = f'ERROR: test for console script "{script.name}" '
                        errmsg+= f'failed with output:\n{e.output.decode()}'
                        raise RuntimeError(errmsg)
