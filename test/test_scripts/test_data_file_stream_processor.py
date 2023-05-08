# imports
import time
from config import TEST_CONST  # pylint: disable=import-error

# pylint: disable=import-error
from test_base_classes import TestWithUploadDataFile, TestWithStreamProcessor


class TestDataFileStreamProcessor(TestWithUploadDataFile, TestWithStreamProcessor):
    """
    Class for testing behavior of a DataFileStreamProcessor
    """

    def run_stream_processor_test(self, topic_name, mode):
        """
        Run the stream processor downloading files to memory
        """
        self.create_stream_processor(
            topic_name=topic_name,
            consumer_group_id="test_data_file_stream_processor",
            other_init_kwargs={"mode": mode},
        )
        self.start_stream_processor_thread()
        try:
            # make sure the stream processor can be checked
            self.assertFalse(self.stream_processor.checked)
            self.stream_processor.control_command_queue.put("c")
            self.stream_processor.control_command_queue.put("check")
            time.sleep(1)
            self.assertTrue(self.stream_processor.checked)
            # wait until the file has been processed
            rel_filepath = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(
                TEST_CONST.TEST_DATA_DIR_PATH
            )
            self.wait_for_files_to_be_processed(rel_filepath, timeout_secs=180)
            # make sure the contents of the file are the same as the original
            ref_bytestring = None
            with open(TEST_CONST.TEST_DATA_FILE_2_PATH, "rb") as fp:
                ref_bytestring = fp.read()
            self.assertTrue(
                (TEST_CONST.TEST_DATA_FILE_2_NAME, ref_bytestring)
                in self.stream_processor.completed_filenames_bytestrings
            )
        except Exception as exc:
            raise exc

    def test_data_file_stream_processor_modes_kafka(self):
        """
        Upload a data file and then use a DataFileStreamProcessor to read its data back
        in all three modes
        """
        topic_name = TEST_CONST.TEST_TOPIC_NAMES["test_data_file_stream_processor_kafka"]
        # upload the test file
        self.upload_single_file(
            TEST_CONST.TEST_DATA_FILE_2_PATH,
            topic_name=topic_name,
            rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
        )
        # start up a stream processor to read its data back into memory
        self.run_stream_processor_test(topic_name, "memory")
        # start up a stream processor to read its data back to disk
        self.reset_stream_processor()
        self.run_stream_processor_test_disk(topic_name, "disk")
        # start up a stream processor to read its data back to memory and disk
        self.reset_stream_processor()
        self.run_stream_processor_test_both(topic_name, "both")
        self.success = True  # pylint: disable=attribute-defined-outside-init

    def test_data_file_stream_processor_restart_kafka(self):
        """
        Test restarting a DataFileStreamProcessor from the beginning of the topic
        after failing to process a file
        """
        topic_name = TEST_CONST.TEST_TOPIC_NAMES[
            "test_data_file_stream_processor_restart_kafka"
        ]
        consumer_group_id = "test_data_file_stream_processor_restart"
        # upload the data files
        self.upload_single_file(
            TEST_CONST.TEST_DATA_FILE_PATH,
            topic_name=topic_name,
            rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
        )
        self.upload_single_file(
            TEST_CONST.TEST_DATA_FILE_2_PATH,
            topic_name=topic_name,
            rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
        )
        rel_filepath_1 = TEST_CONST.TEST_DATA_FILE_PATH.relative_to(
            TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH
        )
        rel_filepath_2 = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(
            TEST_CONST.TEST_DATA_DIR_PATH
        )
        # use a stream processor to read their data back into memory one time,
        # deliberately failing the first file
        self.create_stream_processor(
            topic_name=topic_name, consumer_group_id=consumer_group_id
        )
        self.stream_processor.filenames_to_fail = [TEST_CONST.TEST_DATA_FILE_NAME]
        self.start_stream_processor_thread()
        try:
            self.wait_for_files_to_be_processed(rel_filepath_2)
            # make sure the content of the failed file has been added as "None"
            self.assertTrue(
                (TEST_CONST.TEST_DATA_FILE_NAME, None)
                in self.stream_processor.completed_filenames_bytestrings
            )
            # make sure the contents of the successful file in memory match the original
            ref_bytestring = None
            with open(TEST_CONST.TEST_DATA_FILE_2_PATH, "rb") as fp:
                ref_bytestring = fp.read()
            self.assertTrue(
                (TEST_CONST.TEST_DATA_FILE_2_NAME, ref_bytestring)
                in self.stream_processor.completed_filenames_bytestrings
            )
            # read the .csv table to make sure it registers one succeeded and one failed
            time.sleep(1.0)
            self.stream_processor.file_registry.in_progress_table.dump_to_file()
            self.stream_processor.file_registry.succeeded_table.dump_to_file()
            self.assertEqual(
                len(self.stream_processor.file_registry.filepaths_to_rerun), 1
            )
            in_prog_table = self.stream_processor.file_registry.in_progress_table
            in_prog_entries = in_prog_table.obj_addresses_by_key_attr("status")
            succeeded_table = self.stream_processor.file_registry.succeeded_table
            succeeded_entries = succeeded_table.obj_addresses
            self.assertEqual(len(succeeded_entries), 1)
            self.assertEqual(
                len(in_prog_entries[self.stream_processor.file_registry.FAILED]), 1
            )
        except Exception as exc:
            raise exc
        # upload a third file (fake config file)
        third_filepath = TEST_CONST.FAKE_PROD_CONFIG_FILE_PATH
        self.upload_single_file(
            third_filepath, topic_name=topic_name, rootdir=TEST_CONST.TEST_DATA_DIR_PATH
        )
        rel_filepath_3 = third_filepath.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)
        # recreate and re-run the stream processor, allowing it to process all files
        self.reset_stream_processor()
        self.create_stream_processor(
            topic_name=topic_name, consumer_group_id=consumer_group_id
        )
        self.start_stream_processor_thread()
        try:
            # wait for the new file and the previously-failed file to be processed
            self.wait_for_files_to_be_processed([rel_filepath_1, rel_filepath_3])
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
            time.sleep(1.0)
            # read the .csv table to make sure it registers three successful files
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
