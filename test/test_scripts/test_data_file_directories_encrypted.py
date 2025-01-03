# imports
import pathlib, time, filecmp
from openmsistream.utilities.dataclass_table import DataclassTableReadOnly
from openmsistream.data_file_io.actor.file_registry.producer_file_registry import (
    RegistryLineInProgress,
    RegistryLineCompleted,
)
from openmsistream import DataFileUploadDirectory

try:
    from .config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from .base_classes import (
        TestWithKafkaTopics,
        TestWithDataFileUploadDirectory,
        TestWithDataFileDownloadDirectory,
    )
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from base_classes import (
        TestWithKafkaTopics,
        TestWithDataFileUploadDirectory,
        TestWithDataFileDownloadDirectory,
    )


class TestDataFileDirectoriesEncrypted(
    TestWithKafkaTopics,
    TestWithDataFileUploadDirectory,
    TestWithDataFileDownloadDirectory,
):
    """
    Class for testing DataFileUploadDirectory and DataFileDownloadDirectory functions
    """

    TOPIC_NAME = "test_oms_encrypted"

    TOPICS = {
        TOPIC_NAME: {},
        f"{TOPIC_NAME}.keys": {"--partitions": 1},
        f"{TOPIC_NAME}.reqs": {"--partitions": 1},
        f"{TOPIC_NAME}.subs": {"--partitions": 1},
    }

    def test_encrypted_upload_and_download_kafka(self):
        """
        Test sending and receiving encrypted messages
        """
        # create the upload directory
        self.create_upload_directory(cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_ENC)
        # start the upload thread
        self.start_upload_thread(
            self.TOPIC_NAME, chunk_size=16 * TEST_CONST.TEST_CHUNK_SIZE
        )
        # copy the test file into the watched directory
        test_rel_filepath = (
            pathlib.Path(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME)
            / TEST_CONST.TEST_DATA_FILE_NAME
        )
        self.copy_file_to_watched_dir(TEST_CONST.TEST_DATA_FILE_PATH, test_rel_filepath)
        # start up the DataFileDownloadDirectory
        self.create_download_directory(
            cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,
            topic_name=self.TOPIC_NAME,
            consumer_group_id=f"test_encrypted_data_file_directories_{TEST_CONST.PY_VERSION}",
        )
        self.start_download_thread()
        time.sleep(10)
        try:
            # put the "check" command into the input queues a couple times to test them
            self.upload_directory.control_command_queue.put("c")
            self.download_directory.control_command_queue.put("c")
            self.upload_directory.control_command_queue.put("check")
            self.download_directory.control_command_queue.put("check")
            # wait for the timeout for the test file to be completely reconstructed
            self.wait_for_files_to_reconstruct(test_rel_filepath, timeout_secs=300)
            # shut down the upload directory
            self.stop_upload_thread()
            # make sure the reconstructed file exists with the same name and content as the original
            reco_fp = self.reco_dir / test_rel_filepath
            self.assertTrue(reco_fp.is_file())
            if not filecmp.cmp(TEST_CONST.TEST_DATA_FILE_PATH, reco_fp, shallow=False):
                errmsg = (
                    "ERROR: files are not the same after reconstruction! "
                    "(This may also be due to the timeout being too short)"
                )
                raise RuntimeError(errmsg)
            # make sure that the ProducerFileRegistry files were created
            # and they list the file as completely uploaded
            log_subdir = self.watched_dir / DataFileUploadDirectory.LOG_SUBDIR_NAME
            in_prog_filepath = log_subdir / f"upload_to_{self.TOPIC_NAME}_in_progress.csv"
            completed_filepath = log_subdir / f"uploaded_to_{self.TOPIC_NAME}.csv"
            self.assertTrue(in_prog_filepath.is_file())
            in_prog_table = DataclassTableReadOnly(
                RegistryLineInProgress, filepath=in_prog_filepath, logger=self.logger
            )
            self.assertFalse(in_prog_table.obj_addresses_by_key_attr("filename"))
            self.assertTrue(completed_filepath.is_file())
            completed_table = DataclassTableReadOnly(
                RegistryLineCompleted, filepath=completed_filepath, logger=self.logger
            )
            addrs_by_fp = completed_table.obj_addresses_by_key_attr("rel_filepath")
            self.assertTrue(test_rel_filepath in addrs_by_fp)
        except Exception as exc:
            raise exc
        self.success = True  # pylint: disable=attribute-defined-outside-init
