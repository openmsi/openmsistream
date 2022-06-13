#imports
import sys, os, pathlib, shutil, ctypes.util
from subprocess import CalledProcessError
from .config import SERVICE_CONST
from .utilities import run_cmd_in_subprocess
from .service_manager_base import ServiceManagerBase

class WindowsServiceManager(ServiceManagerBase) :
    """
    Class for working with Windows Services
    """

    @property
    def env_var_names(self):
        for evn in super().env_var_names :
            yield evn
        #get the names of environment variables from the env_var file
        if self.env_var_filepath.is_file() :
            with open(self.env_var_filepath,'r') as fp :
                lines = fp.readlines()
            for line in lines :
                try :
                    linestrip = line.strip()
                    if linestrip!='' :
                        yield linestrip
                except :
                    pass

    def install_service(self) :
        super().install_service()
        #if it doesn't exist there yet, copy the libsodium.dll file to C:\Windows\system32
        self.__copy_libsodium_dll_to_system32()
        #find or install NSSM to run the executable
        self.__find_install_NSSM()
        #run NSSM to install the service
        cmd = f'{SERVICE_CONST.NSSM_PATH} install {self.service_name} \"{sys.executable}\" \"{self.exec_fp}\"'
        run_cmd_in_subprocess(['powershell.exe',cmd],logger=self.logger)
        run_cmd_in_subprocess(['powershell.exe',
                               f'{SERVICE_CONST.NSSM_PATH} set {self.service_name} DisplayName {self.service_name}'],
                               logger=self.logger)
        self.logger.info(f'Done installing {self.service_name}')

    def start_service(self) :
        self.logger.info(f'Starting {self.service_name}...')
        #start the service using net
        cmd = f'net start {self.service_name}'
        run_cmd_in_subprocess(['powershell.exe',cmd],logger=self.logger)
        self.logger.info(f'Done starting {self.service_name}')

    def service_status(self) :
        #find or install NSSM in the current directory
        self.__find_install_NSSM()
        #get the service status
        cmd = f'{SERVICE_CONST.NSSM_PATH} status {self.service_name}'
        result = run_cmd_in_subprocess(['powershell.exe',cmd],logger=self.logger)
        self.logger.info(f'{self.service_name} status: {result.decode()}')

    def stop_service(self) :
        self.logger.info(f'Stopping {self.service_name}...')
        #stop the service using net
        cmd = f'net stop {self.service_name}'
        run_cmd_in_subprocess(['powershell.exe',cmd],logger=self.logger)
        self.logger.info(f'Done stopping {self.service_name}')

    def remove_service(self,remove_env_vars=False,remove_install_args=False,remove_nssm=False) :
        self.logger.info(f'Removing {self.service_name}...')
        #find or install NSSM in the current directory
        self.__find_install_NSSM()
        #using NSSM
        cmd = f'{SERVICE_CONST.NSSM_PATH} remove {self.service_name} confirm'
        run_cmd_in_subprocess(['powershell.exe',cmd],logger=self.logger)
        self.logger.info('Service removed')
        #remove NSSM if requested
        if remove_nssm :
            if SERVICE_CONST.NSSM_PATH.is_file() :
                try :
                    run_cmd_in_subprocess(['powershell.exe',f'del {SERVICE_CONST.NSSM_PATH}'],logger=self.logger)
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

    def __copy_libsodium_dll_to_system32(self) :
        """
        Ensure that the libsodium.dll file exists in C:\Windows\system32
        (Needed to properly load it when running as a service)
        """
        system32_path = pathlib.Path(r'C:\Windows\system32')/'libsodium.dll'
        if system32_path.is_file() :
            return
        current_env_dll = ctypes.util.find_library('libsodium')
        if current_env_dll is not None :
            shutil.copy(pathlib.Path(current_env_dll),system32_path)
        else :
            errmsg = f'ERROR: could not locate libsodium DLL to copy to system32 folder for {self.service_name}!'
            self.logger.error(errmsg,FileNotFoundError)

    def __find_install_NSSM(self,move_local=True) :
        """
        Ensure the NSSM executable exists in the expected location.
        If it exists in the current directory, move it to the expected location.
        If it doesn't exist anywhere, try to download it from the web, but that's finicky in powershell.
        """
        if SERVICE_CONST.NSSM_PATH.is_file() :
            return
        else :
            if (pathlib.Path()/SERVICE_CONST.NSSM_PATH.name).is_file() and move_local :
                (pathlib.Path()/SERVICE_CONST.NSSM_PATH.name).replace(SERVICE_CONST.NSSM_PATH)
                return self.__find_install_NSSM(move_local=False)
            SERVICE_CONST.LOGGER.info(f'Installing NSSM from {SERVICE_CONST.NSSM_DOWNLOAD_URL}...')
            nssm_zip_file_name = SERVICE_CONST.NSSM_DOWNLOAD_URL.split('/')[-1]
            run_cmd_in_subprocess(['powershell.exe',
                                   '[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12'],
                                   logger=self.logger)
            cmd_tuples = [
                (f'curl {SERVICE_CONST.NSSM_DOWNLOAD_URL} -O',
                f'Invoke-WebRequest -Uri {SERVICE_CONST.NSSM_DOWNLOAD_URL} -OutFile {nssm_zip_file_name}'),
                (f'tar -xf {pathlib.Path() / nssm_zip_file_name}',
                f'Expand-Archive {nssm_zip_file_name} -DestinationPath {pathlib.Path().resolve()}'),
                (f'del {nssm_zip_file_name}',
                f'Remove-Item -Path {nssm_zip_file_name}'),
                (f'move {pathlib.Path() / nssm_zip_file_name.rstrip(".zip") / "win64" / "nssm.exe"} {pathlib.Path()}',
                f'''Move-Item -Path {pathlib.Path()/nssm_zip_file_name.rstrip(".zip")/"win64"/"nssm.exe"} \
                    -Destination {SERVICE_CONST.NSSM_PATH}'''),
                (f'rmdir /S /Q {nssm_zip_file_name.rstrip(".zip")}',
                f'Remove-Item -Recurse -Force {nssm_zip_file_name.rstrip(".zip")}'),
            ]
            for cmd in cmd_tuples :
                try :
                    run_cmd_in_subprocess(['powershell.exe',cmd[1]],logger=self.logger)
                except CalledProcessError :
                    run_cmd_in_subprocess(cmd[0],shell=True,logger=self.logger)
            SERVICE_CONST.LOGGER.debug('Done installing NSSM')
