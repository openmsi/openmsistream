#imports
import unittest, platform, shutil, pathlib, logging, time
from subprocess import check_output
from openmsistream.shared.logging import Logger
from openmsistream.services.config import SERVICE_CONST
from openmsistream.services.utilities import run_cmd_in_subprocess
from openmsistream.services.windows_service_manager import WindowsServiceManager
from openmsistream.services.linux_service_manager import LinuxServiceManager
from config import TEST_CONST

#constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.ERROR)

class TestServices(unittest.TestCase) :
    """
    Class for testing that Services can be installed/started/stopped/removed without any errors on Linux OS
    """

    def setUp(self) :
        self.dirs_to_delete = [TEST_CONST.TEST_DIR_SERVICES_TEST]
        for dirpath in self.dirs_to_delete :
            if not dirpath.is_dir() :
                dirpath.mkdir(parents=True)
        self.argslists_by_class_name = {
                'UploadDataFile':[TEST_CONST.TEST_DATA_FILE_PATH,],
                'DataFileUploadDirectory':[TEST_CONST.TEST_DIR_SERVICES_TEST,],
                'DataFileDownloadDirectory':[TEST_CONST.TEST_DIR_SERVICES_TEST,],
                'LecroyFileUploadDirectory':[TEST_CONST.TEST_DIR_SERVICES_TEST,
                                             '--config',TEST_CONST.TEST_CONFIG_FILE_PATH,
                                             '--topic_name',TEST_CONST.TEST_TOPIC_NAMES['test_pdv_plots']],
                'PDVPlotMaker':[TEST_CONST.TEST_DIR_SERVICES_TEST,
                                '--config',TEST_CONST.TEST_CONFIG_FILE_PATH,
                                '--topic_name',TEST_CONST.TEST_TOPIC_NAMES['test_pdv_plots'],
                                '--consumer_group_id','create_new'],
                'OSNStreamProcessor':['phy210127-bucket01',
                                      '--logger_file',TEST_CONST.TEST_DIR_SERVICES_TEST/'test_osn_stream_processor.log',
                                      '--config',TEST_CONST.TEST_CONFIG_FILE_PATH_OSN,
                                      '--topic_name',TEST_CONST.TEST_TOPIC_NAMES['test_osn'],
                                      '--consumer_group_id','create_new'],
            }

    def tearDown(self) :
        for dirpath in self.dirs_to_delete :
            if dirpath.is_dir() :
                shutil.rmtree(dirpath)

    @unittest.skipIf(platform.system()!='Windows','test can only be run on Windows')
    def test_windows_services_kafka(self) :
        """
        Make sure every possible Windows service can be installed, started, checked, stopped, removed, and reinstalled
        """
        self.assertTrue(len(SERVICE_CONST.AVAILABLE_SERVICES)>0)
        for sd in SERVICE_CONST.AVAILABLE_SERVICES :
            try :
                service_class_name = sd['class'].__name__
                if service_class_name not in self.argslists_by_class_name.keys() :
                    raise ValueError(f'ERROR: no arguments to use found for class "{service_class_name}"!')
                service_name = service_class_name+'_test'
                argslist_to_use = []
                for arg in self.argslists_by_class_name[service_class_name] :
                    argslist_to_use.append(str(arg))
                manager = WindowsServiceManager(service_name,
                                                service_class_name=service_class_name,
                                                argslist=argslist_to_use,
                                                interactive=False,
                                                logger=LOGGER)
                manager.install_service()
                for run_mode in ('start','status','stop','remove','reinstall') :
                    time.sleep(1)
                    manager.run_manage_command(run_mode,False,False)
                time.sleep(1)
                error_log_path = pathlib.Path().resolve()/f'{service_name}{SERVICE_CONST.ERROR_LOG_STEM}'
                self.assertFalse(error_log_path.is_file())
            except Exception as e :
                raise e
            finally :
                fps_to_unlink = [(SERVICE_CONST.WORKING_DIR/f'{service_name}_env_vars.txt'),
                                 (SERVICE_CONST.WORKING_DIR/f'{service_name}_install_args.txt')]
                for fp in fps_to_unlink :
                    if fp.exists() :
                        fp.unlink() 
    
    @unittest.skipIf(platform.system()!='Linux' or 
                     check_output(['ps','--no-headers','-o','comm','1']).decode().strip()!='systemd',
                     'test requires systemd running on Linux')
    def test_linux_services_kafka(self) :
        """
        Make sure every possible Linux service can be installed, started, checked, stopped, removed, and reinstalled
        """
        self.assertTrue(len(SERVICE_CONST.AVAILABLE_SERVICES)>0)
        for sd in SERVICE_CONST.AVAILABLE_SERVICES :
            try :
                service_class_name = sd['class'].__name__
                if service_class_name not in self.argslists_by_class_name.keys() :
                    raise ValueError(f'ERROR: no arguments to use found for class "{service_class_name}"!')
                service_name = service_class_name+'_test'
                argslist_to_use = []
                for arg in self.argslists_by_class_name[service_class_name] :
                    argslist_to_use.append(str(arg))
                manager = LinuxServiceManager(service_name,
                                                service_class_name=service_class_name,
                                                argslist=argslist_to_use,
                                                interactive=False,
                                                logger=LOGGER)
                manager.install_service()
                for run_mode in ('start','status','stop','remove','reinstall') :
                    time.sleep(1)
                    manager.run_manage_command(run_mode,False,False)
                time.sleep(1)
                self.assertFalse((SERVICE_CONST.DAEMON_SERVICE_DIR/f'{service_name}.service').exists())
                error_log_path = pathlib.Path().resolve()/f'{service_name}{SERVICE_CONST.ERROR_LOG_STEM}'
                self.assertFalse(error_log_path.is_file())
            except Exception as e :
                raise e
            finally :
                if (SERVICE_CONST.DAEMON_SERVICE_DIR/f'{service_name}.service').exists() :
                    run_cmd_in_subprocess(['sudo',
                                           'rm',
                                           str((SERVICE_CONST.DAEMON_SERVICE_DIR/f'{service_name}.service'))],
                                           logger=LOGGER)
                fps_to_unlink = [(SERVICE_CONST.WORKING_DIR/f'{service_name}_env_vars.txt'),
                                 (SERVICE_CONST.WORKING_DIR/f'{service_name}_install_args.txt'),
                                 (SERVICE_CONST.WORKING_DIR/f'{self.service_name}.service')]
                for fp in fps_to_unlink :
                    if fp.exists() :
                        fp.unlink() 
