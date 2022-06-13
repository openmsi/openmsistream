#imports
import pathlib, importlib, pkg_resources
from inspect import isclass
from ..shared.logging import Logger

class ServicesConstants :
    """
    Constants for working with services
    """

    def __init__(self) :
        #make the Service dictionaries to use
        self.service_dicts = []
        for script in pkg_resources.iter_entry_points('console_scripts') :
            if script.dist.key == 'openmsistream' :
                if script.name in ('InstallService','ManageService','ProvisionNode') :
                    continue
                scriptstr = str(script)
                cmd = (scriptstr.split())[0]
                path = ((scriptstr.split())[2].split(':'))[0]
                funcname = (((scriptstr.split())[2]).split(':'))[1]
                module = importlib.import_module(path)
                run_classes = [getattr(module,x) for x in dir(module) 
                               if isclass(getattr(module,x)) and getattr(module,x).__name__==script.name]
                if len(run_classes)!=1 :
                    errmsg = f'ERROR: could not determine class for script {cmd} in file {path}! '
                    errmsg+= f'Possibilities found: {run_classes}'
                    raise RuntimeError(errmsg)
                self.service_dicts.append({'script_name':cmd,
                                           'class':run_classes[0],
                                           'filepath':path,
                                           'func_name':funcname})
        #make the logger to use
        self.logger = Logger('Services',logger_filepath=self.WORKING_DIR/'Services.log')

    @property
    def WORKING_DIR(self) :
        return (pathlib.Path(__file__).parent/'working_dir').resolve()
    @property
    def AVAILABLE_SERVICES(self) :
        return self.service_dicts # A dictionary with details of the services that are available
    @property
    def LOGGER(self) :
        return self.logger # A shared logger object to use with a constant file
    @property
    def NSSM_DOWNLOAD_URL(self) :
        return 'https://nssm.cc/release/nssm-2.24.zip' # The URL to use for downloading NSSM when needed
    @property
    def NSSM_PATH(self) :
        return (self.WORKING_DIR / 'nssm.exe')
    @property
    def ERROR_LOG_STEM(self) :
        return '_ERROR_LOG.txt'
    @property
    def SERVICE_EXECUTABLE_NAME_STEM(self) :
        return '_service_executable.py'
    @property
    def DAEMON_SERVICE_DIR(self) :
        return pathlib.Path('/etc/systemd/system/')

SERVICE_CONST = ServicesConstants()