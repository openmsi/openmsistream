# imports
import pathlib, filecmp

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


class TestUndecryptableMessagesFlag(
    TestWithKafkaTopics,
    TestWithDataFileUploadDirectory,
    TestWithDataFileDownloadDirectory,
):
    """Test behavior of messages that fail to be decrypted with the
    "treat_undecryptable_as_plaintext" flag added
    """

    TOPIC_NAME = "test_oms_undecryptable_messages_flag"

    TOPICS = {
        TOPIC_NAME: {},
        f"{TOPIC_NAME}.keys": {"--partitions": 1},
        f"{TOPIC_NAME}.reqs": {"--partitions": 1},
        f"{TOPIC_NAME}.subs": {"--partitions": 1},
    }

    def test_undecryptable_messages_flag_kafka(self):
        """
        Test a Consumer receiving messages that are not decryptable
        """
        # create the upload directory to send unencrypted messages
        self.create_upload_directory(cfg_file=TEST_CONST.TEST_CFG_FILE_PATH)
        # start the upload thread
        self.start_upload_thread(
            self.TOPIC_NAME, chunk_size=32 * TEST_CONST.TEST_CHUNK_SIZE
        )
        # copy the test file into the watched directory
        test_rel_filepath = (
            pathlib.Path(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME)
            / TEST_CONST.TEST_DATA_FILE_NAME
        )
        self.copy_file_to_watched_dir(TEST_CONST.TEST_DATA_FILE_PATH, test_rel_filepath)
        # shut down the upload directory
        self.stop_upload_thread()
        # run a DataFileDownloadDirectory with the "treat_undecryptable_as_plaintext"
        # flag set to True
        consumer_group_id = f"test_undecrypted_messages_flag_{TEST_CONST.PY_VERSION}"
        self.create_download_directory(
            cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,
            topic_name=self.TOPIC_NAME,
            consumer_group_id=consumer_group_id,
            treat_undecryptable_as_plaintext=True,
            max_wait_per_decrypt=5.0,
        )
        self.start_download_thread()
        self.wait_for_files_to_reconstruct(test_rel_filepath, timeout_secs=300)
        reco_fp = self.reco_dir / test_rel_filepath
        self.assertTrue(reco_fp.is_file())
        if not filecmp.cmp(TEST_CONST.TEST_DATA_FILE_PATH, reco_fp, shallow=False):
            errmsg = (
                "ERROR: files are not the same after reconstruction! "
                "(This may also be due to the timeout being too short)"
            )
            raise RuntimeError(errmsg)
        self.success = True  # pylint: disable=attribute-defined-outside-init
