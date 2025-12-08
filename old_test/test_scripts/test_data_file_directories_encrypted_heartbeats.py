# imports
import pathlib, time, filecmp, datetime, json
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
        TestWithHeartbeats,
        TestWithDataFileUploadDirectory,
        TestWithDataFileDownloadDirectory,
    )
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from base_classes import (
        TestWithHeartbeats,
        TestWithDataFileUploadDirectory,
        TestWithDataFileDownloadDirectory,
    )


class TestDataFileDirectoriesEncryptedHeartbeats(
    TestWithHeartbeats,
    TestWithDataFileUploadDirectory,
    TestWithDataFileDownloadDirectory,
):
    """
    Class for testing encrypted DataFileUploadDirectory and DataFileDownloadDirectory
    functions while using heartbeats for both
    """

    TOPIC_NAME = "test_oms_encrypted_heartbeats"
    HEARTBEAT_TOPIC_NAME_P = f"{TOPIC_NAME}.heartbeatsp"
    HEARTBEAT_TOPIC_NAME_C = f"{TOPIC_NAME}.heartbeatsc"

    TOPICS = {
        TOPIC_NAME: {},
        f"{TOPIC_NAME}.keys": {"--partitions": 1},
        f"{TOPIC_NAME}.reqs": {"--partitions": 1},
        f"{TOPIC_NAME}.subs": {"--partitions": 1},
        HEARTBEAT_TOPIC_NAME_P: {"--partitions": 1},
        HEARTBEAT_TOPIC_NAME_C: {"--partitions": 1},
    }

    def test_encrypted_upload_and_download_heartbeats_kafka(self):
        """
        Test sending and receiving encrypted messages with heartbeats
        """
        producer_program_id = "upload_directory"
        consumer_program_id = "download_directory"
        # create the upload directory
        self.create_upload_directory(
            cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_ENC,
            heartbeat_topic_name=self.HEARTBEAT_TOPIC_NAME_P,
            heartbeat_program_id=producer_program_id,
            heartbeat_interval_secs=1,
        )
        # start the upload thread
        start_time = datetime.datetime.now()
        chunk_size = 16 * TEST_CONST.TEST_CHUNK_SIZE
        self.start_upload_thread(self.TOPIC_NAME, chunk_size=chunk_size)
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
            heartbeat_topic_name=self.HEARTBEAT_TOPIC_NAME_C,
            heartbeat_program_id=consumer_program_id,
            heartbeat_interval_secs=1,
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
            # and validate heartbeats before closing thread to alow for key exchange
            self.wait_for_files_to_reconstruct(
                test_rel_filepath,
                timeout_secs=300,
                before_close_callback=lambda *args: self.validate_heartbeats(
                    producer_program_id, consumer_program_id, start_time, chunk_size
                ),
            )
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

    def validate_heartbeats(
        self, producer_program_id, consumer_program_id, start_time, chunk_size
    ):
        """Validate that the producer and consumer both successfully sent heartbeat
        messages with the right structure and content
        """
        # validate the producer heartbeats
        producer_heartbeat_msgs = self.get_heartbeat_messages(
            TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS_ENC,
            self.HEARTBEAT_TOPIC_NAME_P,
            producer_program_id,
        )
        self.assertTrue(len(producer_heartbeat_msgs) > 0)
        total_msgs_produced = 0
        total_bytes_produced = 0
        for msg in producer_heartbeat_msgs:
            msg_dict = json.loads(msg.value)
            msg_timestamp = datetime.datetime.strptime(
                msg_dict["timestamp"], self.TIMESTAMP_FMT
            )
            self.assertTrue(msg_timestamp > start_time)
            total_msgs_produced += msg_dict["n_messages_produced"]
            total_bytes_produced += msg_dict["n_bytes_produced"]
        test_file_size = TEST_CONST.TEST_DATA_FILE_PATH.stat().st_size
        test_file_n_chunks = int(test_file_size / chunk_size)
        self.assertTrue(total_msgs_produced >= test_file_n_chunks)
        self.assertTrue(total_bytes_produced >= test_file_size)
        # validate the consumer heartbeats
        consumer_heartbeat_msgs = self.get_heartbeat_messages(
            TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS_ENC_2,
            self.HEARTBEAT_TOPIC_NAME_C,
            consumer_program_id,
        )
        self.assertTrue(len(consumer_heartbeat_msgs) > 0)
        total_msgs_read = 0
        total_msgs_processed = 0
        total_bytes_read = 0
        total_bytes_processed = 0
        for msg in consumer_heartbeat_msgs:
            msg_dict = json.loads(msg.value)
            msg_timestamp = datetime.datetime.strptime(
                msg_dict["timestamp"], self.TIMESTAMP_FMT
            )
            self.assertTrue(msg_timestamp > start_time)
            total_msgs_read += msg_dict["n_messages_read"]
            total_msgs_processed += msg_dict["n_messages_processed"]
            total_bytes_read += msg_dict["n_bytes_read"]
            total_bytes_processed += msg_dict["n_bytes_processed"]
        self.assertTrue(total_msgs_read >= test_file_n_chunks)
        self.assertTrue(total_msgs_processed >= test_file_n_chunks)
        self.assertTrue(total_bytes_read >= test_file_size)
        self.assertTrue(total_bytes_processed >= test_file_size)
