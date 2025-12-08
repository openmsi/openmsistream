# imports
import pathlib

try:
    from .config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from .base_classes import (
        TestWithKafkaTopics,
        TestWithDataFileUploadDirectory,
        TestWithStreamProcessor,
    )
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from base_classes import (
        TestWithKafkaTopics,
        TestWithDataFileUploadDirectory,
        TestWithStreamProcessor,
    )


class TestDataFileStreamProcessorEncrypted(
    TestWithKafkaTopics,
    TestWithStreamProcessor,
    TestWithDataFileUploadDirectory,
):
    """
    Class for testing behavior of an encrypted DataFileStreamProcessor
    """

    TOPIC_NAME = "test_data_file_stream_processor_encrypted"

    TOPICS = {
        TOPIC_NAME: {},
        f"{TOPIC_NAME}.keys": {"--partitions": 1},
        f"{TOPIC_NAME}.reqs": {"--partitions": 1},
        f"{TOPIC_NAME}.subs": {"--partitions": 1},
    }

    def test_data_file_stream_processor_restart_encrypted_kafka(self):
        """
        Test restarting an encrypted DataFileStreamProcessor after failing to process a file
        """
        consumer_group_id = (
            f"test_data_file_stream_processor_restart_encrypted_{TEST_CONST.PY_VERSION}"
        )
        # create and start the upload directory
        self.create_upload_directory(cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_ENC)
        self.start_upload_thread(
            topic_name=self.TOPIC_NAME, chunk_size=16 * TEST_CONST.TEST_CHUNK_SIZE
        )
        # copy the test files into the watched directory
        rel_filepath_1 = (
            pathlib.Path(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME)
            / TEST_CONST.TEST_DATA_FILE_NAME
        )
        rel_filepath_2 = pathlib.Path(TEST_CONST.TEST_DATA_FILE_2_PATH.name)
        self.copy_file_to_watched_dir(TEST_CONST.TEST_DATA_FILE_PATH, rel_filepath_1)
        self.copy_file_to_watched_dir(TEST_CONST.TEST_DATA_FILE_2_PATH)
        # use a stream processor to read their data back into memory one time,
        # deliberately failing the first file
        self.create_stream_processor(
            cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,
            topic_name=self.TOPIC_NAME,
            output_dir=self.output_dir / "stream_processor_output",
            consumer_group_id=consumer_group_id,
        )
        self.stream_processor.filenames_to_fail = [TEST_CONST.TEST_DATA_FILE_NAME]
        self.start_stream_processor_thread()
        try:
            self.wait_for_files_to_be_processed(rel_filepath_2, timeout_secs=300)
            # make sure the content of the failed file has been added as "None"
            self.assertTrue(
                (TEST_CONST.TEST_DATA_FILE_NAME, None)
                in self.stream_processor.completed_filenames_bytestrings
            )
            # make sure the contents of the successful file in memory are the same as the original
            ref_bytestring = None
            with open(TEST_CONST.TEST_DATA_FILE_2_PATH, "rb") as fp:
                ref_bytestring = fp.read()
            self.assertTrue(
                (TEST_CONST.TEST_DATA_FILE_2_NAME, ref_bytestring)
                in self.stream_processor.completed_filenames_bytestrings
            )
            # read the file registry to make sure it registers one file each succeeded and failed
            self.stream_processor.file_registry.in_progress_table.dump_to_file()
            self.stream_processor.file_registry.succeeded_table.dump_to_file()
            self.assertEqual(
                len(self.stream_processor.file_registry.filepaths_to_rerun), 1
            )
            in_prog_table = self.stream_processor.file_registry.in_progress_table
            in_prog_entries = in_prog_table.obj_addresses_by_key_attr("status")
            succeeded_table = self.stream_processor.file_registry.succeeded_table
            succeeded_entries = succeeded_table.obj_addresses
            # allow greater than in case of a previously-failed test
            self.assertTrue(len(succeeded_entries) >= 1)
            self.assertEqual(
                len(in_prog_entries[self.stream_processor.file_registry.FAILED]), 1
            )
        except Exception as exc:
            raise exc
        # upload a third file (fake config file)
        third_filepath = TEST_CONST.FAKE_PROD_CONFIG_FILE_PATH
        rel_filepath_3 = pathlib.Path(third_filepath.name)
        self.copy_file_to_watched_dir(third_filepath)
        # recreate and re-run the stream processor, allowing it to process all files this time
        self.reset_stream_processor()
        self.create_stream_processor(
            cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,
            topic_name=self.TOPIC_NAME,
            output_dir=self.output_dir / "stream_processor_output",
            consumer_group_id=consumer_group_id,
        )
        self.start_stream_processor_thread()
        try:
            self.wait_for_files_to_be_processed(
                [rel_filepath_1, rel_filepath_3], timeout_secs=300
            )
            self.stop_upload_thread()
            # make sure the content of the previously failed file is now correct
            ref_bytestring = None
            with open(TEST_CONST.TEST_DATA_FILE_PATH, "rb") as fp:
                ref_bytestring = fp.read()
            self.assertTrue(
                (TEST_CONST.TEST_DATA_FILE_NAME, ref_bytestring)
                in self.stream_processor.completed_filenames_bytestrings
            )
            # make sure the previously-successful file wasn't read again
            self.assertFalse(
                TEST_CONST.TEST_DATA_FILE_2_NAME
                in [t[0] for t in self.stream_processor.completed_filenames_bytestrings]
            )
            # make sure the contents of the third file are also accurate
            ref_bytestring = None
            with open(third_filepath, "rb") as fp:
                ref_bytestring = fp.read()
            self.assertTrue(
                (third_filepath.name, ref_bytestring)
                in self.stream_processor.completed_filenames_bytestrings
            )
            # read the file registry to make sure it registers three successful files
            self.stream_processor.file_registry.in_progress_table.dump_to_file()
            self.stream_processor.file_registry.succeeded_table.dump_to_file()
            succeeded_table = self.stream_processor.file_registry.succeeded_table
            succeeded_entries = succeeded_table.obj_addresses
            # >3 if the topic has files from previous runs in it
            self.assertTrue(len(succeeded_entries) >= 3)
            succeeded_entries = succeeded_table.obj_addresses_by_key_attr("filename")
            self.assertTrue(len(succeeded_entries[TEST_CONST.TEST_DATA_FILE_2_NAME]) >= 1)
        except Exception as exc:
            raise exc
        self.success = True  # pylint: disable=attribute-defined-outside-init
