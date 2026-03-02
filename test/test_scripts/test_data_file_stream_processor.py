# test_scripts/test_data_file_stream_processor.py
import time
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
@pytest.mark.parametrize("kafka_topics", [TOPICS], indirect=True)
@pytest.mark.usefixtures("logger", "kafka_topics")
def test_data_file_stream_processor_modes_kafka(
    stream_processor_helper, upload_file_helper
):
    upload_file_helper(
        TEST_CONST.TEST_DATA_FILE_2_PATH,
        topic_name=TOPIC_NAME,
        rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
    )

    sp = stream_processor_helper
    rel_filepath = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(
        TEST_CONST.TEST_DATA_DIR_PATH
    )

    with open(TEST_CONST.TEST_DATA_FILE_2_PATH, "rb") as fp:
        ref_bytestring = fp.read()

    for mode in ("memory", "disk", "both"):
        sp["create_stream_processor"](
            topic_name=TOPIC_NAME,
            consumer_group_id=(
                f"test_data_file_stream_processor_{mode}_{TEST_CONST.PY_VERSION}"
            ),
            other_init_kwargs={"mode": mode},
        )
        sp["start_stream_processor_thread"]()

        # verify _on_check is triggered by the "check" control command
        assert not sp["state"]["stream_processor"].checked
        sp["state"]["stream_processor"].control_command_queue.put("c")
        sp["state"]["stream_processor"].control_command_queue.put("check")
        counter = 0
        while not sp["state"]["stream_processor"].checked and counter < 10:
            time.sleep(0.1)
            counter += 1
        assert sp["state"]["stream_processor"].checked

        sp["wait_for_files_to_be_processed"](rel_filepath, timeout_secs=180)

        # verify file contents match the original
        assert (
            TEST_CONST.TEST_DATA_FILE_2_NAME,
            ref_bytestring,
        ) in sp[
            "state"
        ]["stream_processor"].completed_filenames_bytestrings

        sp["reset_stream_processor"](remove_output=True)


@pytest.mark.kafka
@pytest.mark.parametrize("kafka_topics", [{TOPIC_2_NAME: {}}], indirect=True)
@pytest.mark.usefixtures("kafka_topics")
def test_data_file_stream_processor_restart_kafka(
    stream_processor_helper, upload_file_helper
):
    sp = stream_processor_helper
    consumer_group_id = f"test_data_file_stream_processor_restart_{TEST_CONST.PY_VERSION}"

    rel_filepath_1 = TEST_CONST.TEST_DATA_FILE_PATH.relative_to(
        TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH
    )
    rel_filepath_2 = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(
        TEST_CONST.TEST_DATA_DIR_PATH
    )

    # upload files 1 and 2
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

    # first run: deliberately fail file 1
    sp["create_stream_processor"](
        topic_name=TOPIC_2_NAME, consumer_group_id=consumer_group_id
    )
    sp["state"]["stream_processor"].filenames_to_fail = [TEST_CONST.TEST_DATA_FILE_NAME]
    sp["start_stream_processor_thread"]()
    sp["wait_for_files_to_be_processed"](rel_filepath_2)

    # verify file 1 recorded as failed, file 2 recorded with correct contents
    assert (
        TEST_CONST.TEST_DATA_FILE_NAME,
        None,
    ) in sp[
        "state"
    ]["stream_processor"].completed_filenames_bytestrings
    with open(TEST_CONST.TEST_DATA_FILE_2_PATH, "rb") as fp:
        ref_bytestring_2 = fp.read()
    assert (
        TEST_CONST.TEST_DATA_FILE_2_NAME,
        ref_bytestring_2,
    ) in sp[
        "state"
    ]["stream_processor"].completed_filenames_bytestrings

    # verify registry: 1 succeeded, 1 failed, 1 queued for rerun
    time.sleep(1.0)
    sp["state"]["stream_processor"].file_registry.in_progress_table.dump_to_file()
    sp["state"]["stream_processor"].file_registry.succeeded_table.dump_to_file()
    assert len(sp["state"]["stream_processor"].file_registry.filepaths_to_rerun) == 1
    in_progress_table = sp["state"]["stream_processor"].file_registry.in_progress_table
    in_prog_entries = in_progress_table.obj_addresses_by_key_attr("status")
    succeeded_table = sp["state"]["stream_processor"].file_registry.succeeded_table
    assert len(succeeded_table.obj_addresses) == 1
    assert len(in_prog_entries[sp["state"]["stream_processor"].file_registry.FAILED]) == 1

    # upload a third file before resetting so it lands in the topic
    third_filepath = TEST_CONST.FAKE_PROD_CONFIG_FILE_PATH
    upload_file_helper(
        third_filepath,
        topic_name=TOPIC_2_NAME,
        rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
    )
    rel_filepath_3 = third_filepath.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)

    sp["reset_stream_processor"]()

    # second run: same consumer group — should re-process file 1 and pick up file 3
    sp["create_stream_processor"](
        topic_name=TOPIC_2_NAME, consumer_group_id=consumer_group_id
    )
    sp["start_stream_processor_thread"]()
    sp["wait_for_files_to_be_processed"]([rel_filepath_1, rel_filepath_3])

    # verify file 1 now processed correctly
    with open(TEST_CONST.TEST_DATA_FILE_PATH, "rb") as fp:
        ref_bytestring_1 = fp.read()
    assert (
        TEST_CONST.TEST_DATA_FILE_NAME,
        ref_bytestring_1,
    ) in sp[
        "state"
    ]["stream_processor"].completed_filenames_bytestrings

    # verify file 2 was NOT re-processed by the second run
    assert TEST_CONST.TEST_DATA_FILE_2_NAME not in [
        t[0] for t in sp["state"]["stream_processor"].completed_filenames_bytestrings
    ]

    # verify file 3 processed correctly
    with open(third_filepath, "rb") as fp:
        ref_bytestring_3 = fp.read()
    assert (
        third_filepath.name,
        ref_bytestring_3,
    ) in sp[
        "state"
    ]["stream_processor"].completed_filenames_bytestrings

    # verify registry: >= 3 succeeded total, file 2 appears at least once
    time.sleep(1.0)
    sp["state"]["stream_processor"].file_registry.in_progress_table.dump_to_file()
    sp["state"]["stream_processor"].file_registry.succeeded_table.dump_to_file()
    succeeded_table = sp["state"]["stream_processor"].file_registry.succeeded_table
    assert len(succeeded_table.obj_addresses) >= 3
    succeeded_by_filename = succeeded_table.obj_addresses_by_key_attr("filename")
    assert len(succeeded_by_filename[TEST_CONST.TEST_DATA_FILE_2_NAME]) >= 1

    sp["reset_stream_processor"](remove_output=True)
