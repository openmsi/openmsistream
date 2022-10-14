"""Manage Linux daemons"""

#imports
import sys, os, pathlib, textwrap
from .config import SERVICE_CONST
from .utilities import run_cmd_in_subprocess
from .service_manager_base import ServiceManagerBase

class LinuxServiceManager(ServiceManagerBase) :
    """
    Base class for working with Linux daemons

    :param service_name: The name of the daemon as installed
    :type service_name: str
    :param service_spec_string: A string specifying which code should be run as a daemon.
        Could be the name of an OpenMSIStream Runnable class, or the path to a custom Python code.
        Custom Services can also specify a :class:`~.workflow.Runnable` class name,
        and/or a function in the file using special formatting like [class_name]=[path.to.file]:[function_name].
        Only needed to initially install the daemon.
    :type service_spec_string: str, optional
    :param argslist: The list of arguments (as from the command line) to pass to the
        :class:`~.workflow.Runnable` class.
        Only needed to initially install the daemon.
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
                linesplit = (line.strip()).split('=')
                if len(linesplit)==2 :
                    env_var_names.add(linesplit[0])
        for evn in env_var_names :
            yield evn

    def __init__(self,*args,**kwargs) :
        super().__init__(*args,**kwargs)
        self.daemon_working_dir_filepath = SERVICE_CONST.WORKING_DIR/f'{self.service_name}.service'
        self.daemon_filepath = SERVICE_CONST.DAEMON_SERVICE_DIR/self.daemon_working_dir_filepath.name

    def install_service(self) :
        """
        Install the daemon
        """
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
        """
        Start the daemon
        """
        self.logger.info(f'Starting {self.service_name}...')
        self.__check_systemd_installed()
        run_cmd_in_subprocess(['sudo','systemctl','daemon-reload'],logger=self.logger)
        run_cmd_in_subprocess(['sudo','systemctl','start',f'{self.service_name}.service'],logger=self.logger)
        self.logger.info(f'Done starting {self.service_name}')

    def service_status(self) :
        """
        Print the status of the daemon
        """
        self.__check_systemd_installed()
        result = run_cmd_in_subprocess(['sudo','systemctl','status',f'{self.service_name}.service'],logger=self.logger)
        self.logger.info(f'{self.service_name} status: {result.decode()}')

    def stop_service(self) :
        """
        Stop the daemon
        """
        self.logger.info(f'Stopping {self.service_name}...')
        self.__check_systemd_installed()
        run_cmd_in_subprocess(['sudo','systemctl','stop',f'{self.service_name}.service'],logger=self.logger)
        self.logger.info(f'Done stopping {self.service_name}')

    def remove_service(self,remove_env_vars=False,remove_install_args=False,remove_nssm=False) :
        """
        Remove the daemon.

        :param remove_env_vars: if True, any environment variables needed by the daemon will be removed.
        :type remove_env_vars: bool, optional.
        :param remove_install_args: if True, the file listing the arguments used to install the daemon
            (to make it easier to re-install) will be removed.
        :type remove_install_args: bool, optional
        :param remove_nssm: Not used for Linux daemons.
        :type remove_nssm: bool, optional
        """
        self.logger.info(f'Removing {self.service_name}...')
        self.__check_systemd_installed()
        try :
            run_cmd_in_subprocess(['sudo','systemctl','disable',f'{self.service_name}.service'],logger=self.logger)
        except Exception :
            pass
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
            SERVICE_CONST.logger.info(f'Creating a new daemon service directory at {SERVICE_CONST.DAEMON_SERVICE_DIR}')
            SERVICE_CONST.DAEMON_SERVICE_DIR.mkdir(parents=True)
        #write out the file pointing to the python executable
        description = 'Python script'
        if self.service_dict['class'] is not None :
            classdoc = self.service_dict['class'].__doc__
            if classdoc is not None :
                description = classdoc.strip().replace('\n',' ')
            else :
                description+=f" for a {self.service_dict['class']} object"
        code = f'''\
            [Unit]
            Description = {description}
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
