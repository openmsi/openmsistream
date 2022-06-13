#imports
import sys, os, pathlib, textwrap
from .config import SERVICE_CONST
from .utilities import run_cmd_in_subprocess
from .service_manager_base import ServiceManagerBase

class LinuxServiceManager(ServiceManagerBase) :
    """
    Class for working with Linux Services/daemons
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
                    linesplit = (line.strip()).split('=')
                    if len(linesplit)==2 :
                        yield linesplit[0]
                except :
                    pass

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        self.daemon_working_dir_filepath = SERVICE_CONST.WORKING_DIR/f'{self.service_name}.service'
        self.daemon_filepath = SERVICE_CONST.DAEMON_SERVICE_DIR/self.daemon_working_dir_filepath.name

    def install_service(self) :
        super().install_service()
        #make sure systemd is running
        self.__check_systemd_installed()
        #write the daemon file pointing to the executable
        self.__write_daemon_file()
        #enable the service
        run_cmd_in_subprocess(['sudo','systemctl','daemon-reload'],logger=self.logger)
        run_cmd_in_subprocess(['sudo','systemctl','enable',f'{self.service_name}.service'],logger=self.logger)
        self.logger.info(f'Done installing {self.service_name}')

    def start_service(self) :
        self.logger.info(f'Starting {self.service_name}...')
        self.__check_systemd_installed()
        run_cmd_in_subprocess(['sudo','systemctl','daemon-reload'],logger=self.logger)
        run_cmd_in_subprocess(['sudo','systemctl','start',f'{self.service_name}.service'],logger=self.logger)
        self.logger.info(f'Done starting {self.service_name}')

    def service_status(self) :
        self.__check_systemd_installed()
        result = run_cmd_in_subprocess(['sudo','systemctl','status',f'{self.service_name}.service'],logger=self.logger)
        self.logger.info(f'{self.service_name} status: {result.decode()}')

    def stop_service(self) :
        self.logger.info(f'Stopping {self.service_name}...')
        self.__check_systemd_installed()
        run_cmd_in_subprocess(['sudo','systemctl','stop',f'{self.service_name}.service'],logger=self.logger)
        self.logger.info(f'Done stopping {self.service_name}')

    def remove_service(self,remove_env_vars=False,remove_install_args=False,remove_nssm=False) :
        self.logger.info(f'Removing {self.service_name}...')
        self.__check_systemd_installed()
        run_cmd_in_subprocess(['sudo','systemctl','disable',f'{self.service_name}.service'],logger=self.logger)
        if self.daemon_filepath.exists() :
            run_cmd_in_subprocess(['sudo','rm','-f',str(self.daemon_filepath)],logger=self.logger)
        run_cmd_in_subprocess(['sudo','systemctl','daemon-reload'],logger=self.logger)
        run_cmd_in_subprocess(['sudo','systemctl','reset-failed'],logger=self.logger)
        self.logger.info('Service removed')
        if remove_nssm :
            warnmsg = "WARNING: requested to remove NSSM along with the Service, "
            warnmsg+= "but Linux Services don't install NSSM so nothing was done."
            self.logger.warning(warnmsg)
        super().remove_service(remove_env_vars=remove_env_vars,remove_install_args=remove_install_args)

    def _write_env_var_file(self) :
        code = ''
        for evn in self.env_var_names :
            val = os.path.expandvars(f'${evn}')
            if val==f'${evn}' :
                raise RuntimeError(f'ERROR: value not found for expected environment variable {evn}!')
            code += f'{evn}={val}\n'
        if code=='' :
            return False
        with open(self.env_var_filepath,'w') as fp :
            fp.write(code)
        run_cmd_in_subprocess(['chmod','go-rwx',self.env_var_filepath])
        return True 

    def __check_systemd_installed(self) :
        """
        Raises an error if systemd is not installed (systemd is needed to control Linux daemons)
        """
        check = run_cmd_in_subprocess(['ps','--no-headers','-o','comm','1'],logger=self.logger)
        if check.decode().rstrip()!='systemd' :
            errmsg = 'ERROR: Installing programs as Services ("daemons") on Linux requires systemd!'
            errmsg = 'You can install systemd with "sudo apt install systemd" (or similar) and try again.'
            self.logger.error(errmsg,RuntimeError)

    def __write_daemon_file(self) :
        """
        Write the Unit/.service file to the daemon directory that calls the Python executable
        """
        #make sure the directory to hold the file exists
        if not SERVICE_CONST.DAEMON_SERVICE_DIR.is_dir() :
            SERVICE_CONST.LOGGER.info(f'Creating a new daemon service directory at {SERVICE_CONST.DAEMON_SERVICE_DIR}')
            SERVICE_CONST.DAEMON_SERVICE_DIR.mkdir(parents=True)
        #write out the file pointing to the python executable
        code = f'''\
            [Unit]
            Description = {self.service_dict['class'].__doc__.strip()}
            Requires = network-online.target remote-fs.target
            After = network-online.target remote-fs.target

            [Service]
            Type = simple
            User = {os.path.expandvars('$USER')}
            ExecStart = {sys.executable} {self.exec_fp}'''
        if self.env_vars_needed :
            code+=f'''\n\
            EnvironmentFile = {self.env_var_filepath}'''
        code+=f'''\n\
            WorkingDirectory = {pathlib.Path().resolve()}
            Restart = on-failure
            RestartSec = 30

            [Install]
            WantedBy = multi-user.target'''
        with open(self.daemon_working_dir_filepath,'w') as fp :
            fp.write(textwrap.dedent(code))
        run_cmd_in_subprocess(['sudo','mv',str(self.daemon_working_dir_filepath),str(self.daemon_filepath.parent)],
                              logger=self.logger)
