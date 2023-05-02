# imports
import pathlib, shutil, filecmp
from openmsistream.utilities.dataclass_table import DataclassTableReadOnly
from openmsistream.data_file_io.actor.file_registry.producer_file_registry import (
    RegistryLineInProgress,
    RegistryLineCompleted,
)
from openmsistream.data_file_io.actor.data_file_upload_directory import (
    DataFileUploadDirectory,
)
from config import TEST_CONST
from test_case_classes import (
    TestWithDataFileUploadDirectory,
    TestWithDataFileDownloadDirectory,
    TestWithEnvVars,
)

# constants
TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES[pathlib.Path(__file__).name[: -len(".py")]]


class TestDataFileDirectories(
    TestWithDataFileUploadDirectory, TestWithDataFileDownloadDirectory, TestWithEnvVars
):
    """
    Class for testing DataFileUploadDirectory and DataFileDownloadDirectory functions
    """

    def run_data_file_upload_directory(self):
        """
        Called by the test method below to run an upload directory from start to finish
        """
        # create the upload directory with the default config file
        self.create_upload_directory()
        # start it running in a new thread
        self.start_upload_thread(topic_name=TOPIC_NAME)
        try:
            # put the test file in the watched directory
            dest_rel_path = (
                pathlib.Path(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME)
                / TEST_CONST.TEST_DATA_FILE_NAME
            )
            self.copy_file_to_watched_dir(TEST_CONST.TEST_DATA_FILE_PATH, dest_rel_path)
            # put the "check" command into the input queue a couple times to test it
            self.upload_directory.control_command_queue.put("c")
            self.upload_directory.control_command_queue.put("check")
            # shut down the upload thread
            self.stop_upload_thread()
            # make sure that the ProducerFileRegistry files were created
            # and they list the file as completely uploaded
            log_subdir = self.watched_dir / DataFileUploadDirectory.LOG_SUBDIR_NAME
            in_prog_filepath = log_subdir / f"upload_to_{TOPIC_NAME}_in_progress.csv"
            completed_filepath = log_subdir / f"uploaded_to_{TOPIC_NAME}.csv"
            self.assertTrue(in_prog_filepath.is_file())
            in_prog_table = DataclassTableReadOnly(
                RegistryLineInProgress,
                filepath=in_prog_filepath,
                logger=self.logger,
            )
            self.assertEqual(in_prog_table.obj_addresses_by_key_attr("filename"), {})
            self.assertTrue(completed_filepath.is_file())
            completed_table = DataclassTableReadOnly(
                RegistryLineCompleted,
                filepath=completed_filepath,
                logger=self.logger,
            )
            addrs_by_fp = completed_table.obj_addresses_by_key_attr("rel_filepath")
            self.assertTrue(dest_rel_path in addrs_by_fp.keys())
        except Exception as e:
            raise e

    def run_data_file_download_directory(self):
        """
        Called by the test method below to run a download directory from start to finish
        """
        # create the download directory
        self.create_download_directory(
            consumer_group_id="run_data_file_download_directory"
        )
        # start reconstruct in a separate thread so we can time it out
        self.start_download_thread()
        try:
            # put the "check" command into the input queue a couple times
            self.download_directory.control_command_queue.put("c")
            self.download_directory.control_command_queue.put("check")
            # wait for the timeout for the test file to be completely reconstructed
            reco_rel_fp = pathlib.Path(
                TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME / TEST_CONST.TEST_DATA_FILE_NAME
            )
            self.wait_for_files_to_reconstruct(reco_rel_fp)
            # make sure the reconstructed file exists with the same name and content as the original
            fp = (
                self.reco_dir
                / TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME
                / TEST_CONST.TEST_DATA_FILE_NAME
            )
            self.assertTrue(fp.is_file())
            if not filecmp.cmp(TEST_CONST.TEST_DATA_FILE_PATH, fp, shallow=False):
                errmsg = (
                    "ERROR: files are not the same after reconstruction! "
                    "(This may also be due to the timeout being too short)"
                )
                raise RuntimeError(errmsg)
        except Exception as e:
            raise e

    def test_upload_and_download_directories_kafka(self):
        """
        Test both upload_files_as_added and then reconstruct, in that order
        """
        self.run_data_file_upload_directory()
        self.run_data_file_download_directory()
        self.success = True

    def test_filepath_should_be_uploaded(self):
        # create an upload directory
        self.create_upload_directory(TEST_CONST.TEST_DATA_DIR_PATH)
        # test the "filepath_should_be_uploaded" function
        self.log_at_info("\nExpecting three errors below:")
        with self.assertRaises(TypeError):
            self.upload_directory.filepath_should_be_uploaded(None)
        with self.assertRaises(TypeError):
            self.upload_directory.filepath_should_be_uploaded(5)
        with self.assertRaises(TypeError):
            self.upload_directory.filepath_should_be_uploaded(
                "this is a string not a path!"
            )
        self.assertFalse(
            self.upload_directory.filepath_should_be_uploaded(
                TEST_CONST.TEST_DATA_DIR_PATH / ".this_file_is_hidden"
            )
        )
        self.assertFalse(
            self.upload_directory.filepath_should_be_uploaded(
                TEST_CONST.TEST_DATA_DIR_PATH / "this_file_is_a_log_file.log"
            )
        )
        for fp in TEST_CONST.TEST_DATA_DIR_PATH.rglob("*"):
            check = True
            if fp.is_dir():
                check = False
            elif fp.name.startswith(".") or fp.name.endswith(".log"):
                check = False
            self.assertEqual(
                self.upload_directory.filepath_should_be_uploaded(fp.resolve()), check
            )
        subdir_path = (
            TEST_CONST.TEST_DATA_DIR_PATH / "this_subdirectory_should_not_be_uploaded"
        )
        subdir_path.mkdir()
        try:
            self.assertFalse(
                self.upload_directory.filepath_should_be_uploaded(subdir_path)
            )
        except Exception as e:
            raise e
        finally:
            shutil.rmtree(subdir_path)
        self.success = True
