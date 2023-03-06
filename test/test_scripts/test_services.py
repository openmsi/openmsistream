#imports
import unittest, platform, shutil, pathlib, logging, time
from subprocess import check_output
from openmsistream.utilities.logging import Logger
from openmsistream.services.config import SERVICE_CONST
from openmsistream.services.utilities import run_cmd_in_subprocess
from openmsistream.services.windows_service_manager import WindowsServiceManager
from openmsistream.services.linux_service_manager import LinuxServiceManager
from openmsistream.services.install_service import main as install_service_main
from openmsistream.services.manage_service import main as manage_service_main
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
                'S3TransferStreamProcessor':[
                    'phy210127-bucket01',
                    '--output_dir',TEST_CONST.TEST_DIR_SERVICES_TEST,
                    '--config',TEST_CONST.TEST_CFG_FILE_PATH_S3,
                    '--topic_name',TEST_CONST.TEST_TOPIC_NAMES['test_s3_transfer_stream_processor'],
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
        self.assertTrue(len(SERVICE_CONST.available_services)>0)
        for sd in SERVICE_CONST.available_services :
            try :
                service_class_name = sd['class'].__name__
                if service_class_name not in self.argslists_by_class_name.keys() :
                    raise ValueError(f'ERROR: no arguments to use found for class "{service_class_name}"!')
                service_name = service_class_name+'_test'
                argslist_to_use = []
                for arg in self.argslists_by_class_name[service_class_name] :
                    argslist_to_use.append(str(arg))
                manager = WindowsServiceManager(service_name,
                                                service_spec_string=service_class_name,
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
        self.assertTrue(len(SERVICE_CONST.available_services)>0)
        for sd in SERVICE_CONST.available_services :
            try :
                service_class_name = sd['class'].__name__
                if service_class_name not in self.argslists_by_class_name.keys() :
                    raise ValueError(f'ERROR: no arguments to use found for class "{service_class_name}"!')
                service_name = service_class_name+'_test'
                argslist_to_use = []
                for arg in self.argslists_by_class_name[service_class_name] :
                    argslist_to_use.append(str(arg))
                manager = LinuxServiceManager(service_name,
                                                service_spec_string=service_class_name,
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
                service_path = SERVICE_CONST.DAEMON_SERVICE_DIR/f'{service_name}.service'
                if service_path.exists() :
                    run_cmd_in_subprocess(['sudo','rm',f'"{service_path}"'],logger=LOGGER)
                fps_to_unlink = [(SERVICE_CONST.WORKING_DIR/f'{service_name}_env_vars.txt'),
                                 (SERVICE_CONST.WORKING_DIR/f'{service_name}_install_args.txt'),
                                 (SERVICE_CONST.WORKING_DIR/f'{service_name}.service')]
                for fp in fps_to_unlink :
                    if fp.exists() :
                        fp.unlink() 

    @unittest.skipIf((platform.system()!='Windows') and 
                     (platform.system()!='Linux' or 
                     check_output(['ps','--no-headers','-o','comm','1']).decode().strip()!='systemd'),
                     'test can only be run on Windows or on Linux with systemd installed')
    def test_custom_runnable_service(self) :
        """
        Make sure the example custom Runnable service can be installed, 
        started, checked, stopped, removed, and reinstalled
        """
        service_name = 'RunnableExampleServiceTest'
        test_file_path = TEST_CONST.TEST_DIR_CUSTOM_RUNNABLE_SERVICE_TEST/'runnable_example_service_test.txt'
        error_log_path = pathlib.Path().resolve()/f'{service_name}{SERVICE_CONST.ERROR_LOG_STEM}'
        service_path = SERVICE_CONST.DAEMON_SERVICE_DIR/f'{service_name}.service'
        if test_file_path.exists() :
            test_file_path.unlink()
        try :
            install_service_main(
                ['RunnableExample=openmsistream.services.examples.runnable_example',
                str(test_file_path.parent.resolve()),
                '--service_name',service_name]
            )
            for run_mode in ('start','status','stop','remove','reinstall') :
                manage_service_main([service_name,run_mode])
                time.sleep(5 if run_mode in ('start','reinstall') else 1)
            self.assertTrue(test_file_path.is_file())
            self.assertFalse(error_log_path.exists())
            if platform.system()=='Linux' :
                self.assertFalse(service_path.exists())
        except Exception as e :
            raise e
        finally :
            fps_to_unlink = [test_file_path,error_log_path,
                             (SERVICE_CONST.WORKING_DIR/f'{service_name}_env_vars.txt'),
                             (SERVICE_CONST.WORKING_DIR/f'{service_name}_install_args.txt')]
            if platform.system()=='Linux' : 
                if service_path.exists() :
                    run_cmd_in_subprocess(['sudo','rm',f'"{service_path}"'],logger=LOGGER)
                fps_to_unlink.append(SERVICE_CONST.WORKING_DIR/f'{service_name}.service')
            for fp in fps_to_unlink :
                if fp.exists() :
                    fp.unlink() 
            if TEST_CONST.TEST_DIR_CUSTOM_RUNNABLE_SERVICE_TEST.is_dir() :
                shutil.rmtree(TEST_CONST.TEST_DIR_CUSTOM_RUNNABLE_SERVICE_TEST)

    @unittest.skipIf((platform.system()!='Windows') and 
                     (platform.system()!='Linux' or 
                     check_output(['ps','--no-headers','-o','comm','1']).decode().strip()!='systemd'),
                     'test can only be run on Windows or on Linux with systemd installed')
    def test_custom_script_service(self) :
        """
        Make sure the example custom standalone script service can be installed, 
        started, checked, stopped, removed, and reinstalled
        """
        service_name = 'ScriptExampleServiceTest'
        test_file_path = TEST_CONST.TEST_DIR_CUSTOM_SCRIPT_SERVICE_TEST/'script_example_service_test.txt'
        error_log_path = pathlib.Path().resolve()/f'{service_name}{SERVICE_CONST.ERROR_LOG_STEM}'
        if test_file_path.exists() :
            test_file_path.unlink()
        try :
            install_service_main(
                ['openmsistream.services.examples.script_example:main',
                str(test_file_path.parent.resolve()),
                '--service_name',service_name]
            )
            for run_mode in ('start','status','stop','remove','reinstall') :
                manage_service_main([service_name,run_mode])
                time.sleep(5 if run_mode in ('start','reinstall') else 1)
            self.assertTrue(test_file_path.is_file())
            self.assertFalse(error_log_path.exists())
            if platform.system()=='Linux' :
                self.assertFalse((SERVICE_CONST.DAEMON_SERVICE_DIR/f'{service_name}.service').exists())
        except Exception as e :
            raise e
        finally :
            fps_to_unlink = [test_file_path,error_log_path,
                             (SERVICE_CONST.WORKING_DIR/f'{service_name}_env_vars.txt'),
                             (SERVICE_CONST.WORKING_DIR/f'{service_name}_install_args.txt')]
            if platform.system()=='Linux' :
                if (SERVICE_CONST.DAEMON_SERVICE_DIR/f'{service_name}.service').exists() :
                    run_cmd_in_subprocess(['sudo',
                                           'rm',
                                           str((SERVICE_CONST.DAEMON_SERVICE_DIR/f'{service_name}.service'))],
                                           logger=LOGGER)
                fps_to_unlink.append(SERVICE_CONST.WORKING_DIR/f'{service_name}.service')
            for fp in fps_to_unlink :
                if fp.exists() :
                    fp.unlink() 
            if TEST_CONST.TEST_DIR_CUSTOM_SCRIPT_SERVICE_TEST.is_dir() :
                shutil.rmtree(TEST_CONST.TEST_DIR_CUSTOM_SCRIPT_SERVICE_TEST)
