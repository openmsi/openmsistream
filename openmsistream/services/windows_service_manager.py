"""Manage Windows Services"""

#imports
import sys, os, pathlib, shutil, ctypes.util
from subprocess import CalledProcessError
from ..utilities.misc import change_dir
from .config import SERVICE_CONST
from .utilities import run_cmd_in_subprocess
from .service_manager_base import ServiceManagerBase

class WindowsServiceManager(ServiceManagerBase) :
    """
    Base class for working with Windows Services

    :param service_name: The name of the Service as installed
    :type service_name: str
    :param service_spec_string: A string specifying which code should be run as a Service.
        Could be the name of an OpenMSIStream Runnable class, or the path to a custom Python code.
        Custom Services can also specify a :class:`~.utilities.Runnable` class name,
        and/or a function in the file using special formatting like [class_name]=[path.to.file]:[function_name].
        Only needed to initially install the Service.
    :type service_spec_string: str, optional
    :param argslist: The list of arguments (as from the command line) to pass to the
        :class:`~.utilities.Runnable` class.
        Only needed to initially install the Service.
    :type argslist: list, optional
    :param interactive: if True, a few more messages/prompts will come up telling a user what to do
    :type interactive: bool, optional
    """

    @property
    def env_var_names(self):
        env_var_names = super().env_var_names
        #get the names of environment variables from the env_var file
        if self.env_var_filepath.is_file() :
            with open(self.env_var_filepath,'r') as fp :
                lines = fp.readlines()
            for line in lines :
                linestrip = line.strip()
                if linestrip!='' :
                    env_var_names.add(linestrip)
        for evn in env_var_names :
            yield evn

    def install_service(self) :
        """
        Install the Service
        """
        super().install_service()
        #if it doesn't exist there yet, copy the libsodium.dll file to C:\Windows\system32
        self.__copy_lib_dlls_to_system32()
        #find or install NSSM to run the executable
        self.__find_install_nssm()
        #run NSSM to install the service
        with change_dir(SERVICE_CONST.NSSM_PATH.parent) :
            cmd = f'./{SERVICE_CONST.NSSM_PATH.name} install {self.service_name} "{sys.executable}" '
            if ' ' in str(self.exec_fp) :
                cmd+= f'"\""""{self.exec_fp}\"""'
            else :
                cmd+= f'\"{self.exec_fp}\"'
            run_cmd_in_subprocess(['powershell.exe',cmd],logger=self.logger)
            cmd = f'./{SERVICE_CONST.NSSM_PATH.name} set {self.service_name} DisplayName {self.service_name}'
            run_cmd_in_subprocess(['powershell.exe',cmd],logger=self.logger)
            cmd = f'./{SERVICE_CONST.NSSM_PATH.name} set {self.service_name} DependOnService "Network Connections"'
            run_cmd_in_subprocess(['powershell.exe',cmd],logger=self.logger)
        self.logger.info(f'Done installing {self.service_name}')

    def start_service(self) :
        """
        Start the Service
        """
        self.logger.info(f'Starting {self.service_name}...')
        #start the service using net
        cmds = ['powershell.exe',f'net start {self.service_name}']
        run_cmd_in_subprocess(cmds,logger=self.logger)
        self.logger.info(f'Done starting {self.service_name}')

    def service_status(self) :
        """
        Print the status of the Service
        """
        #find or install NSSM in the current directory
        self.__find_install_nssm()
        #get the service status
        with change_dir(SERVICE_CONST.NSSM_PATH.parent) :
            cmds = ['powershell.exe',f'./{SERVICE_CONST.NSSM_PATH.name} status {self.service_name}']
            result = run_cmd_in_subprocess(cmds,logger=self.logger)
        self.logger.info(f'{self.service_name} status: {result.decode()}')

    def stop_service(self) :
        """
        Stop the Service
        """
        self.logger.info(f'Stopping {self.service_name}...')
        #stop the service using net
        cmds = ['powershell.exe',f'net stop {self.service_name}']
        run_cmd_in_subprocess(cmds,logger=self.logger)
        self.logger.info(f'Done stopping {self.service_name}')

    def remove_service(self,remove_env_vars=False,remove_install_args=False,remove_nssm=False) :
        """
        Remove the Service

        :param remove_env_vars: if True, any environment variables needed by the Service will be removed.
        :type remove_env_vars: bool, optional.
        :param remove_install_args: if True, the file listing the arguments used to install the Service
            (to make it easier to re-install) will be removed.
        :type remove_install_args: bool, optional
        :param remove_nssm: if True, the NSSM executable will be removed.
        :type remove_nssm: bool, optional
        """
        self.logger.info(f'Removing {self.service_name}...')
        #find or install NSSM in the current directory
        self.__find_install_nssm()
        #using NSSM
        with change_dir(SERVICE_CONST.NSSM_PATH.parent) :
            cmds = ['powershell.exe',f'./{SERVICE_CONST.NSSM_PATH.name} remove {self.service_name} confirm']
            run_cmd_in_subprocess(cmds,logger=self.logger)
        self.logger.info('Service removed')
        #remove NSSM if requested
        if remove_nssm :
            if SERVICE_CONST.NSSM_PATH.is_file() :
                try :
                    run_cmd_in_subprocess(['powershell.exe',f'del "{SERVICE_CONST.NSSM_PATH}"'],logger=self.logger)
                except CalledProcessError :
                    msg = f'WARNING: failed to delete {SERVICE_CONST.NSSM_PATH}. '
                    msg+= 'You are free to delete it manually if you would like.'
                    self.logger.info(msg)
            else :
                msg = f'NSSM does not exist at {SERVICE_CONST.NSSM_PATH} so it will not be removed'
                self.logger.info(msg)
        else :
            if SERVICE_CONST.NSSM_PATH.is_file() :
                msg = f'NSSM executable at {SERVICE_CONST.NSSM_PATH} will be retained'
                self.logger.info(msg)
        super().remove_service(remove_env_vars=remove_env_vars,remove_install_args=remove_install_args)

    def _write_env_var_file(self) :
        code = ''
        for evn in self.env_var_names :
            val = os.path.expandvars(f'${evn}')
            if val==f'${evn}' :
                raise RuntimeError(f'ERROR: value not found for expected environment variable {evn}!')
            code += f'{evn}\n'
        if code=='' :
            return False
        with open(self.env_var_filepath,'w') as fp :
            fp.write(code)
        return True

    def __copy_lib_dlls_to_system32(self) :
        """
        Ensure that several .dll files exist in C:\\Windows\\system32
        (Needed to properly load it when running as a service)
        """
        system32_path = pathlib.Path(r'C:\Windows\system32')
        package_names = ['librdkafka-a2007a74','librdkafka-09f4f3ec',
                         'libcrypto-1_1-x64-6d3f430c','libcurl-40bef7bc','libssl-1_1-x64-a125c0ba',
                         'zlib1-50deb1cb','zstd-acd7910e',
                         'libcrypto-1_1-x64','libssl-1_1-x64',
                         'libsodium']
        for pname in package_names :
            current_env_dll = ctypes.util.find_library(pname)
            if current_env_dll is not None :
                current_env_dll_path = pathlib.Path(current_env_dll)
                if (system32_path/current_env_dll_path.name).is_file() :
                    continue
                try :
                    shutil.copy(current_env_dll_path,system32_path/current_env_dll_path.name)
                except Exception as exc :
                    warnmsg = f'WARNING: failed to copy {pname} DLL file from {current_env_dll_path} to '
                    warnmsg+= f'{system32_path}. This will likely cause the Python code running as a Service '
                    warnmsg+=  'to crash. Exception will be logged below, but not reraised.'
                    self.logger.warning(warnmsg,exc_info=exc)

    def __find_install_nssm(self,move_local=True) :
        """
        Ensure the NSSM executable exists in the expected location.
        If it exists in the current directory, move it to the expected location.
        If it doesn't exist anywhere, try to download it from the web, but that's finicky in powershell.
        """
        if SERVICE_CONST.NSSM_PATH.is_file() :
            return
        if (pathlib.Path()/SERVICE_CONST.NSSM_PATH.name).is_file() and move_local :
            (pathlib.Path()/SERVICE_CONST.NSSM_PATH.name).replace(SERVICE_CONST.NSSM_PATH)
            self.__find_install_nssm(move_local=False)
        SERVICE_CONST.logger.info(f'Installing NSSM from {SERVICE_CONST.NSSM_DOWNLOAD_URL}...')
        nssm_zip_file_name = SERVICE_CONST.NSSM_DOWNLOAD_URL.rsplit('/', maxsplit=1)[-1]
        run_cmd_in_subprocess(['powershell.exe',
                                '[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12'],
                                logger=self.logger)
        cmd_tuples = [
            (('curl',f'{SERVICE_CONST.NSSM_DOWNLOAD_URL}','-O'),
             f'Invoke-WebRequest -Uri {SERVICE_CONST.NSSM_DOWNLOAD_URL} -OutFile {nssm_zip_file_name}'),
            (('tar','-xf',f'"{pathlib.Path() / nssm_zip_file_name}"'),
             f'Expand-Archive {nssm_zip_file_name} -DestinationPath "{pathlib.Path().resolve()}"'),
            (('del',f'{nssm_zip_file_name}'),
             f'Remove-Item -Path {nssm_zip_file_name}'),
            (('move',f'"{pathlib.Path()/nssm_zip_file_name.rstrip(".zip")/"win64"/"nssm.exe"}"',
              f'"{pathlib.Path()}"'),
             f'''Move-Item -Path "{pathlib.Path()/nssm_zip_file_name.rstrip(".zip")/"win64"/"nssm.exe"}"
                 -Destination "{SERVICE_CONST.NSSM_PATH}"'''),
            (('rmdir','/S','/Q',f'{nssm_zip_file_name.rstrip(".zip")}'),
             f'Remove-Item -Recurse -Force {nssm_zip_file_name.rstrip(".zip")}'),
        ]
        try :
            for cmd in cmd_tuples :
                try :
                    run_cmd_in_subprocess(['powershell.exe',cmd[1]],logger=self.logger)
                except CalledProcessError :
                    run_cmd_in_subprocess(list(cmd[0]),shell=True,logger=self.logger)
        except CalledProcessError :
            pass
        if SERVICE_CONST.NSSM_PATH.is_file() :
            SERVICE_CONST.logger.debug('Done installing NSSM')
        else :
            errmsg =  'ERROR: failed to install NSSM automatically. Please download NSSM from '
            errmsg+= f'{SERVICE_CONST.NSSM_DOWNLOAD_URL}, put the executable in the current directory '
            errmsg+= f'or in {SERVICE_CONST.NSSM_PATH.parent}, and try again.'
            SERVICE_CONST.logger.error(errmsg)
