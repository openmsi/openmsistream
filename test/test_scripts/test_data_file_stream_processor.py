# test_scripts/test_data_file_stream_processor.py
import pytest

from openmsistream import DataFileStreamProcessor

from .config import TEST_CONST


class DataFileStreamProcessorForTesting(DataFileStreamProcessor):
    """
    Class to use for testing DataFileStreamProcessor functions since it's an abstract base class
    """

    def __init__(self, *args, **kwargs):
        self.checked = False
        self.completed_filenames_bytestrings = []
        self.filenames_to_fail = []
        super().__init__(*args, **kwargs)

    def _process_downloaded_data_file(self, datafile, lock):
        # fail any files that have been set to fail
        if datafile.filename in self.filenames_to_fail:
            with lock:
                self.completed_filenames_bytestrings.append((datafile.filename, None))
            return ValueError(
                f"ERROR: testing processing for {datafile.filename} is set to fail!"
            )
        # add the filename and bytestring to the list
        with lock:
            self.completed_filenames_bytestrings.append(
                (datafile.filename, datafile.bytestring)
            )
        return None

    def _failed_processing_callback(self, datafile, lock):
        if datafile.filename not in self.filenames_to_fail:
            raise RuntimeError("ERROR: _failed_processing_callback invoked in test!")

    def _mismatched_hash_callback(self, datafile, lock):
        raise RuntimeError("ERROR: _mismatched_hash_callback invoked in test!")

    def _on_check(self):
        self.checked = True
        super()._on_check()

    @classmethod
    def run_from_command_line(cls, args=None):
        pass


# -------------------
# Tests
# -------------------


TOPIC_NAME = "test_data_file_stream_processor"
TOPIC_2_NAME = "test_data_file_stream_processor_2"

TOPICS = {
    TOPIC_NAME: {},
    TOPIC_2_NAME: {},
}


@pytest.mark.kafka
@pytest.mark.integration
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics")
def test_data_file_stream_processor_modes_kafka(
    stream_processor_helper, upload_file_helper
):
    # upload the test file
    upload_file_helper(
        TEST_CONST.TEST_DATA_FILE_2_PATH,
        topic_name=TOPIC_NAME,
        rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
    )

    sp = stream_processor_helper
    sp["create_stream_processor"](
        topic_name=TOPIC_NAME, other_init_kwargs={"mode": "memory"}
    )
    sp["start_stream_processor_thread"]()
    sp["wait_for_files_to_be_processed"](
        TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)
    )
    sp["reset_stream_processor"](remove_output=True)

    # disk mode
    sp["create_stream_processor"](
        topic_name=TOPIC_NAME, other_init_kwargs={"mode": "disk"}
    )
    sp["start_stream_processor_thread"]()
    sp["wait_for_files_to_be_processed"](
        TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)
    )
    sp["reset_stream_processor"](remove_output=True)

    # both mode
    sp["create_stream_processor"](
        topic_name=TOPIC_NAME, other_init_kwargs={"mode": "both"}
    )
    sp["start_stream_processor_thread"]()
    sp["wait_for_files_to_be_processed"](
        TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)
    )
    sp["reset_stream_processor"](remove_output=True)


@pytest.mark.kafka
@pytest.mark.integration
def test_data_file_stream_processor_restart_kafka(
    stream_processor_helper, upload_file_helper
):
    sp = stream_processor_helper

    # Upload initial files
    upload_file_helper(
        TEST_CONST.TEST_DATA_FILE_PATH,
        topic_name=TOPIC_2_NAME,
        rootdir=TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
    )
    upload_file_helper(
        TEST_CONST.TEST_DATA_FILE_2_PATH,
        topic_name=TOPIC_2_NAME,
        rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
    )

    consumer_group_id = f"test_data_file_stream_processor_restart_{TEST_CONST.PY_VERSION}"

    # create stream processor and fail first file
    sp["create_stream_processor"](
        topic_name=TOPIC_2_NAME, consumer_group_id=consumer_group_id
    )
    sp["state"]["stream_processor"].filenames_to_fail = [TEST_CONST.TEST_DATA_FILE_NAME]
    sp["start_stream_processor_thread"]()
    sp["wait_for_files_to_be_processed"](
        TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)
    )

    # Upload a third file
    third_filepath = TEST_CONST.FAKE_PROD_CONFIG_FILE_PATH
    upload_file_helper(
        third_filepath, topic_name=TOPIC_2_NAME, rootdir=TEST_CONST.TEST_DATA_DIR_PATH
    )

    # Reset and re-run stream processor
    sp["reset_stream_processor"]()
    sp["create_stream_processor"](
        topic_name=TOPIC_2_NAME, consumer_group_id=consumer_group_id
    )
    sp["start_stream_processor_thread"]()
    sp["wait_for_files_to_be_processed"](
        [
            TEST_CONST.TEST_DATA_FILE_PATH.relative_to(
                TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH
            ),
            third_filepath.relative_to(TEST_CONST.TEST_DATA_DIR_PATH),
        ]
    )
    sp["reset_stream_processor"](remove_output=True)
