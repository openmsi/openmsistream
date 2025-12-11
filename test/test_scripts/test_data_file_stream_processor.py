# test_scripts/test_data_file_stream_processor.py
import pathlib
import time
import shutil
import pytest
import datetime
from openmsistream import DataFileStreamProcessor, UploadDataFile

from .config import TEST_CONST
from openmsistream.utilities.config import RUN_CONST

from threading import Thread as ExceptionTrackingThread


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
# Stream processor fixture
# -------------------
@pytest.fixture
def stream_processor_helper(tmp_path, logger):
    """
    Fixture that replicates TestWithStreamProcessor exactly,
    returning helper functions for pytest.
    """
    state = {
        "output_dir": tmp_path,
        "stream_processor": None,
        "stream_processor_thread": None,
        "logger": logger,
    }

    def create_stream_processor(
        stream_processor_type=DataFileStreamProcessorForTesting,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name="test",
        output_dir=None,
        n_threads=RUN_CONST.N_DEFAULT_DOWNLOAD_THREADS,
        consumer_group_id="create_new",
        other_init_args=(),
        other_init_kwargs=None,
    ):
        if state["stream_processor"] is not None:
            raise RuntimeError(
                f"ERROR: stream processor is {state['stream_processor']} but should be None!"
            )
        if output_dir is None:
            output_dir = state["output_dir"]
        if not other_init_kwargs:
            other_init_kwargs = {}
        state["stream_processor"] = stream_processor_type(
            *other_init_args,
            cfg_file,
            topic_name,
            output_dir=output_dir,
            n_threads=n_threads,
            consumer_group_id=consumer_group_id,
            logger=state["logger"],
            **other_init_kwargs,
        )

    def start_stream_processor_thread(func=None, args=(), kwargs=None):
        if state["stream_processor_thread"] is not None:
            errmsg = (
                f"ERROR: stream processor thread is {state['stream_processor_thread']} "
                "but it should be None!"
            )
            raise RuntimeError(errmsg)
        if func is None:
            func = state["stream_processor"].process_files_as_read
        if not kwargs:
            kwargs = {}
        state["stream_processor_thread"] = ExceptionTrackingThread(
            target=func, args=args, kwargs=kwargs
        )
        state["stream_processor_thread"].start()

    def wait_for_files_to_be_processed(rel_filepaths, timeout_secs=90):
        if isinstance(rel_filepaths, pathlib.PurePath):
            rel_filepaths = [rel_filepaths]

        files_found_by_path = {fp: False for fp in rel_filepaths}

        state["logger"].info(
            f"Waiting to process files; will timeout after {timeout_secs} seconds..."
        )
        all_files_found = False
        start_time = datetime.datetime.now()
        while (
            not all_files_found
            and (datetime.datetime.now() - start_time).total_seconds() < timeout_secs
        ):
            current_messages_read = state["stream_processor"].n_msgs_read
            time_waited = (datetime.datetime.now() - start_time).total_seconds()
            state["logger"].info(
                f"\t{current_messages_read} messages read after {time_waited:.2f} seconds...."
            )
            time.sleep(5)
            for rel_fp, found_file in files_found_by_path.items():
                if found_file:
                    continue
                if rel_fp in state["stream_processor"].recent_processed_filepaths:
                    files_found_by_path[rel_fp] = True
            all_files_found = sum(files_found_by_path.values()) == len(rel_filepaths)

        msg = (
            f"Quitting stream processor thread after reading {state['stream_processor'].n_msgs_read} "
            "messages; will timeout after 30 seconds...."
        )
        state["logger"].info(msg)
        state["stream_processor"].control_command_queue.put("q")
        state["stream_processor_thread"].join(timeout=30)
        if state["stream_processor_thread"].is_alive():
            raise TimeoutError(
                "ERROR: stream processor thread timed out after 30 seconds!"
            )

    def reset_stream_processor(remove_output=False):
        sp_thread = state["stream_processor_thread"]
        sp = state["stream_processor"]
        if sp_thread and sp:
            if sp_thread.is_alive():
                try:
                    sp.shutdown()
                    sp_thread.join(timeout=30)
                    if sp_thread.is_alive():
                        raise TimeoutError("Download thread timed out after 30 seconds")
                except Exception as exc:
                    raise exc
        state["stream_processor"] = None
        state["stream_processor_thread"] = None
        if remove_output:
            shutil.rmtree(state["output_dir"])
            state["output_dir"].mkdir(parents=True)

    return {
        "create_stream_processor": create_stream_processor,
        "start_stream_processor_thread": start_stream_processor_thread,
        "wait_for_files_to_be_processed": wait_for_files_to_be_processed,
        "reset_stream_processor": reset_stream_processor,
        "state": state,
    }


# -------------------
# Upload single file fixture
# -------------------
@pytest.fixture
def upload_file_helper(logger):
    """
    Fixture that replicates TestWithUploadDataFile exactly
    """

    def upload_single_file(
        filepath,
        cfg_file=TEST_CONST.TEST_CFG_FILE_PATH,
        topic_name="test",
        rootdir=None,
        n_threads=RUN_CONST.N_DEFAULT_UPLOAD_THREADS,
        chunk_size=TEST_CONST.TEST_CHUNK_SIZE,
    ):
        upload_datafile = UploadDataFile(filepath, rootdir=rootdir, logger=logger)
        upload_datafile.upload_whole_file(
            cfg_file,
            topic_name,
            n_threads=n_threads,
            chunk_size=chunk_size,
        )

    return upload_single_file


# -------------------
# Tests
# -------------------
@pytest.mark.integration
def test_data_file_stream_processor_modes_kafka(
    stream_processor_helper, upload_file_helper
):
    TOPIC_NAME = "test_data_file_stream_processor"

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


@pytest.mark.integration
def test_data_file_stream_processor_restart_kafka(
    stream_processor_helper, upload_file_helper
):
    TOPIC_2_NAME = "test_data_file_stream_processor_2"
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
