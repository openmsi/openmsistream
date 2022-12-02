#imports
import unittest, pathlib, logging, time, shutil, urllib.request
import importlib.machinery, importlib.util
from openmsistream import UploadDataFile
from openmsistream.utilities import Logger
from openmsistream.utilities.exception_tracking_thread import ExceptionTrackingThread
from openmsistream.data_file_io.actor.file_registry.stream_handler_registries import StreamProcessorRegistry
from config import TEST_CONST

#import the XRDCSVPlotter from the examples directory
class_path = TEST_CONST.EXAMPLES_DIR_PATH / 'creating_plots' / 'xrd_csv_plotter.py'
module_name = class_path.name[:-len('.py')]
loader = importlib.machinery.SourceFileLoader(module_name, str(class_path))
module = loader.load_module()

#constants
LOGGER = Logger(pathlib.Path(__file__).name.split('.')[0],logging.ERROR)
TIMEOUT_SECS = 90
JOIN_TIMEOUT_SECS = 60
UPLOAD_FILE = TEST_CONST.EXAMPLES_DIR_PATH / 'creating_plots' / 'SC001_XRR.csv'
TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES[pathlib.Path(__file__).name[:-len('.py')]]
CONSUMER_GROUP_ID = 'test_plots_for_tutorial'

class TestPlotsForTutorial(unittest.TestCase) :
    """
    Class for testing that an uploaded file can be read back from the topic and have its metadata 
    successfully extracted and produced to another topic as a string of JSON
    """

    def setUp(self) :
        """
        Download the test data file from its URL on the PARADIM website
        """
        urllib.request.urlretrieve(TEST_CONST.TUTORIAL_TEST_FILE_URL,
                                   UPLOAD_FILE)

    def tearDown(self) :
        """
        Remove the test data file that was downloaded
        """
        if UPLOAD_FILE.is_file() :
            UPLOAD_FILE.unlink()

    def test_plots_for_tutorial_kafka(self) :
        #remove any old output
        if TEST_CONST.TEST_TUTORIAL_PLOTS_OUTPUT_DIR.is_dir() :
            shutil.rmtree(TEST_CONST.TEST_TUTORIAL_PLOTS_OUTPUT_DIR)
        #start up the plot maker
        plot_maker = module.XRDCSVPlotter(
                                TEST_CONST.TEST_CFG_FILE_PATH,
                                TOPIC_NAME,
                                consumer_group_id=CONSUMER_GROUP_ID,
                                output_dir=TEST_CONST.TEST_TUTORIAL_PLOTS_OUTPUT_DIR,
                                logger=LOGGER,
                            )
        plotting_thread = ExceptionTrackingThread(target=plot_maker.process_files_as_read)
        plotting_thread.start()
        try :
            #upload the test data file
            LOGGER.set_stream_level(logging.INFO)
            msg = 'Uploading test plotting file in test_plots_for_tutorial'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            upload_file = UploadDataFile(UPLOAD_FILE,
                                        logger=LOGGER)
            upload_file.upload_whole_file(TEST_CONST.TEST_CFG_FILE_PATH,TOPIC_NAME)
            #wait for the file to be processed
            current_messages_read = -1
            time_waited = 0
            LOGGER.set_stream_level(logging.INFO)
            msg = f'Waiting to create plots from the "{TOPIC_NAME}" topic in '
            msg+= f'test_plots_for_tutorial (will timeout after {TIMEOUT_SECS} seconds)...'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            recofp = pathlib.Path(UPLOAD_FILE.name)
            while (recofp not in plot_maker.completely_processed_filepaths) and time_waited<TIMEOUT_SECS :
                current_messages_read = plot_maker.n_msgs_read
                LOGGER.set_stream_level(logging.INFO)
                LOGGER.info(f'\t{current_messages_read} messages read after waiting {time_waited} seconds....')
                LOGGER.set_stream_level(logging.ERROR)
                time.sleep(5)
                time_waited+=5
            #put the "quit" command into the input queue, which should stop the thread and clean it up also
            LOGGER.set_stream_level(logging.INFO)
            msg = f'Quitting plotting thread in test_plots_for_tutorial after reading {plot_maker.n_msgs_read} '
            msg+= f'messages; will timeout after {JOIN_TIMEOUT_SECS} seconds....'
            LOGGER.info(msg)
            LOGGER.set_stream_level(logging.ERROR)
            plot_maker.control_command_queue.put('q')
            #wait for the reproducer thread to finish
            plotting_thread.join(timeout=JOIN_TIMEOUT_SECS)
            if plotting_thread.is_alive() :
                errmsg = 'ERROR: download thread in test_plots_for_tutorial timed out after '
                errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                raise TimeoutError(errmsg)
            #make sure the file is listed in the 'results_produced' file
            time.sleep(1.0)
            plot_maker.file_registry.in_progress_table.dump_to_file()
            plot_maker.file_registry.succeeded_table.dump_to_file()
            srpr = StreamProcessorRegistry(
                        dirpath=TEST_CONST.TEST_TUTORIAL_PLOTS_OUTPUT_DIR/module.XRDCSVPlotter.LOG_SUBDIR_NAME,
                        topic_name=TOPIC_NAME,
                        consumer_group_id=CONSUMER_GROUP_ID,
                        logger=LOGGER
                    )
            self.assertEqual(len(srpr.filepaths_to_rerun),0)
            in_prog_table = srpr.in_progress_table
            in_prog_entries = in_prog_table.obj_addresses_by_key_attr('status')
            succeeded_table = srpr.succeeded_table
            succeeded_entries = succeeded_table.obj_addresses
            self.assertTrue(len(succeeded_entries)>=1)
            self.assertTrue(srpr.FAILED not in in_prog_entries.keys())
            #get the attributes of the succeeded file to make sure its the one that was produced
            succeeded_entry_attrs = succeeded_table.get_entry_attrs(succeeded_entries[0])
            self.assertTrue(succeeded_entry_attrs['filename']==UPLOAD_FILE.name)
            #make sure the plot file exists
            self.assertTrue((TEST_CONST.TEST_TUTORIAL_PLOTS_OUTPUT_DIR/(UPLOAD_FILE.stem+'_xrd_plot.png')).is_file())
        except Exception as e :
            raise e
        finally :
            if plotting_thread.is_alive() :
                try :
                    plot_maker.control_command_queue.put('q')
                    plotting_thread.join(timeout=JOIN_TIMEOUT_SECS)
                    if plotting_thread.is_alive() :
                        errmsg = 'ERROR: plotting thread in test_plots_for_tutorial timed out after '
                        errmsg+= f'{JOIN_TIMEOUT_SECS} seconds!'
                        raise TimeoutError(errmsg)
                except Exception as e :
                    raise e
                finally :
                    shutil.rmtree(TEST_CONST.TEST_TUTORIAL_PLOTS_OUTPUT_DIR)
            if TEST_CONST.TEST_TUTORIAL_PLOTS_OUTPUT_DIR.is_dir() :
                shutil.rmtree(TEST_CONST.TEST_TUTORIAL_PLOTS_OUTPUT_DIR)
