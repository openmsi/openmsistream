# imports
import pathlib, shutil, filecmp, re, json, datetime, time
from openmsistream.utilities.dataclass_table import DataclassTableReadOnly
from openmsistream.data_file_io.actor.file_registry.producer_file_registry import (
    RegistryLineInProgress,
    RegistryLineCompleted,
)
from openmsistream.data_file_io.actor.data_file_upload_directory import (
    DataFileUploadDirectory,
)

try:
    from .config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from .base_classes import (
        TestWithHeartbeats,
        TestWithLogs,
        TestWithDataFileUploadDirectory,
        TestWithDataFileDownloadDirectory,
        TestWithEnvVars,
    )
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error,wrong-import-order

    # pylint: disable=import-error,wrong-import-order
    from base_classes import (
        TestWithHeartbeats,
        TestWithLogs,
        TestWithDataFileUploadDirectory,
        TestWithDataFileDownloadDirectory,
        TestWithEnvVars,
    )


class TestDataFileDirectories(
    TestWithHeartbeats,
    TestWithLogs,
    TestWithDataFileUploadDirectory,
    TestWithDataFileDownloadDirectory,
    TestWithEnvVars,
):
    """
    Class for testing DataFileUploadDirectory and DataFileDownloadDirectory functions
    """

    TOPIC_NAME = "test_data_file_directories"
    HEARTBEAT_TOPIC_NAME = "heartbeats"
    LOG_TOPIC_NAME = "logs"

    TOPICS = {
        TOPIC_NAME: {},
        HEARTBEAT_TOPIC_NAME: {"--partitions": 1},
        LOG_TOPIC_NAME: {"--partitions": 1},
    }

    def run_data_file_upload_directory(self, upload_file_dict, **create_kwargs):
        """
        Called by the test method below to run an upload directory from start to finish
        """
        # create the upload directory with the default config file
        self.create_upload_directory(**create_kwargs)
        # start it running in a new thread
        self.start_upload_thread(topic_name=self.TOPIC_NAME)
        try:
            # put the test file(s) in the watched directory
            for filepath, filedict in upload_file_dict.items():
                rootdir = filedict["rootdir"] if "rootdir" in filedict else None
                self.copy_file_to_watched_dir(filepath, filepath.relative_to(rootdir))
            # put the "check" command into the input queue a couple times to test it
            self.upload_directory.control_command_queue.put("c")
            self.upload_directory.control_command_queue.put("check")
            # shut down the upload thread
            self.stop_upload_thread()
            # make sure that the ProducerFileRegistry files were created
            # and they list the file(s) as completely uploaded
            log_subdir = self.watched_dir / DataFileUploadDirectory.LOG_SUBDIR_NAME
            in_prog_filepath = log_subdir / f"upload_to_{self.TOPIC_NAME}_in_progress.csv"
            completed_filepath = log_subdir / f"uploaded_to_{self.TOPIC_NAME}.csv"
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
            for filepath, filedict in upload_file_dict.items():
                if filedict["upload_expected"]:
                    rootdir = filedict["rootdir"] if "rootdir" in filedict else None
                    if rootdir:
                        rel_path = filepath.relative_to(rootdir)
                    else:
                        rel_path = pathlib.Path(filepath.name)
                    self.assertTrue(rel_path in addrs_by_fp)
        except Exception as exc:
            raise exc

    def run_data_file_download_directory(self, download_file_dict, **other_create_kwargs):
        """
        Called by the test method below to run a download directory from start to finish
        """
        # make a list of relative filepaths we'll be waiting for
        relevant_files = {}
        for filepath, filedict in download_file_dict.items():
            if filedict["download_expected"]:
                rootdir = filedict["rootdir"] if "rootdir" in filedict else None
                if rootdir:
                    rel_path = filepath.relative_to(rootdir)
                else:
                    rel_path = pathlib.Path(filepath.name)
                relevant_files[filepath] = rel_path
        # create the download directory
        self.create_download_directory(topic_name=self.TOPIC_NAME, **other_create_kwargs)
        # start reconstruct in a separate thread so we can time it out
        self.start_download_thread()
        try:
            # put the "check" command into the input queue a couple times
            self.download_directory.control_command_queue.put("c")
            self.download_directory.control_command_queue.put("check")
            # wait for the timeout for the test file(s) to be completely reconstructed
            self.wait_for_files_to_reconstruct(relevant_files.values())
            # make sure the reconstructed file(s) exists with the same content as the original
            for orig_fp, rel_fp in relevant_files.items():
                reco_fp = self.reco_dir / rel_fp
                self.assertTrue(reco_fp.is_file())
                if not filecmp.cmp(orig_fp, reco_fp, shallow=False):
                    errmsg = (
                        "ERROR: files are not the same after reconstruction! "
                        "(This may also be due to the timeout being too short)"
                    )
                    raise RuntimeError(errmsg)
        except Exception as exc:
            raise exc

    def test_upload_and_download_directories_kafka(self):
        """
        Test the upload and download directories while applying regular expressions
        """
        files_roots = {
            TEST_CONST.TEST_DATA_FILE_PATH: {
                "rootdir": TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
                "upload_expected": True,
                "download_expected": True,
            },
            TEST_CONST.FAKE_PROD_CONFIG_FILE_PATH: {
                "rootdir": TEST_CONST.TEST_DATA_DIR_PATH,
                "upload_expected": True,
                "download_expected": False,
            },
            TEST_CONST.TEST_METADATA_DICT_PICKLE_FILE: {
                "rootdir": TEST_CONST.TEST_DATA_DIR_PATH,
                "upload_expected": False,
                "download_expected": False,
            },
        }
        upload_regex = re.compile(r"^.*\.(dat|config)$")
        download_regex = re.compile(r"^.*\.dat$")
        self.run_data_file_upload_directory(files_roots, upload_regex=upload_regex)
        consumer_group_id = (
            f"run_data_file_download_directory_with_regexes_{TEST_CONST.PY_VERSION}"
        )
        self.run_data_file_download_directory(
            files_roots,
            consumer_group_id=consumer_group_id,
            filepath_regex=download_regex,
        )
        self.success = True  # pylint: disable=attribute-defined-outside-init

    def test_upload_and_download_directories_heartbeats_kafka(self):
        """
        Test the upload and download directories while sending heartbeats
        """
        files_roots = {
            TEST_CONST.TEST_DATA_FILE_PATH: {
                "rootdir": TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
                "upload_expected": True,
                "download_expected": True,
            },
        }
        producer_program_id = "upload"
        consumer_program_id = "download"
        start_time = datetime.datetime.now()
        self.run_data_file_upload_directory(
            files_roots,
            heartbeat_topic_name=self.HEARTBEAT_TOPIC_NAME,
            heartbeat_program_id=producer_program_id,
            heartbeat_interval_secs=1,
        )
        consumer_group_id = (
            f"run_data_file_download_directory_with_heartbeats_{TEST_CONST.PY_VERSION}"
        )
        self.run_data_file_download_directory(
            files_roots,
            consumer_group_id=consumer_group_id,
            heartbeat_topic_name=self.HEARTBEAT_TOPIC_NAME,
            heartbeat_program_id=consumer_program_id,
            heartbeat_interval_secs=1,
        )
        # validate the producer heartbeats
        producer_heartbeat_msgs = self.get_heartbeat_messages(
            TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
            self.HEARTBEAT_TOPIC_NAME,
            producer_program_id,
        )
        self.assertTrue(len(producer_heartbeat_msgs) > 0)
        total_msgs_produced = 0
        total_bytes_produced = 0
        for msg in producer_heartbeat_msgs:
            msg_dict = json.loads(msg.value())
            msg_timestamp = datetime.datetime.strptime(
                msg_dict["timestamp"], self.TIMESTAMP_FMT
            )
            self.assertTrue(msg_timestamp > start_time)
            total_msgs_produced += msg_dict["n_messages_produced"]
            total_bytes_produced += msg_dict["n_bytes_produced"]
        test_file_size = TEST_CONST.TEST_DATA_FILE_PATH.stat().st_size
        test_file_n_chunks = int(test_file_size / TEST_CONST.TEST_CHUNK_SIZE)
        self.assertTrue(total_msgs_produced >= test_file_n_chunks)
        self.assertTrue(total_bytes_produced >= test_file_size)
        # validate the consumer heartbeats
        consumer_heartbeat_msgs = self.get_heartbeat_messages(
            TEST_CONST.TEST_CFG_FILE_PATH_HEARTBEATS,
            self.HEARTBEAT_TOPIC_NAME,
            consumer_program_id,
        )
        self.assertTrue(len(consumer_heartbeat_msgs) > 0)
        total_msgs_read = 0
        total_msgs_processed = 0
        total_bytes_read = 0
        total_bytes_processed = 0
        for msg in consumer_heartbeat_msgs:
            msg_dict = json.loads(msg.value())
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
        self.success = True  # pylint: disable=attribute-defined-outside-init

    def test_upload_and_download_directories_logs_kafka(self):
        """
        Test the upload and download directories while sending logs
        """
        files_roots = {
            TEST_CONST.TEST_DATA_FILE_PATH: {
                "rootdir": TEST_CONST.TEST_DATA_FILE_ROOT_DIR_PATH,
                "upload_expected": True,
                "download_expected": True,
            },
        }
        producer_program_id = "upload"
        consumer_program_id = "download"
        start_time = time.time()
        self.run_data_file_upload_directory(
            files_roots,
            log_topic_name=self.LOG_TOPIC_NAME,
            log_program_id=producer_program_id,
            log_interval_secs=1,
        )
        consumer_group_id = (
            f"run_data_file_download_directory_with_logs_{TEST_CONST.PY_VERSION}"
        )
        self.run_data_file_download_directory(
            files_roots,
            consumer_group_id=consumer_group_id,
            log_topic_name=self.LOG_TOPIC_NAME,
            log_program_id=consumer_program_id,
            log_interval_secs=1,
        )
        # validate the producer logs
        producer_log_msgs = self.get_log_messages(
            TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
            self.LOG_TOPIC_NAME,
            producer_program_id,
        )
        self.assertTrue(len(producer_log_msgs) > 0)
        for msg in producer_log_msgs:
            msg_dict = json.loads(msg.value())
            self.assertTrue(float(msg_dict["timestamp"]) >= start_time)
        # validate the consumer logs
        consumer_log_msgs = self.get_log_messages(
            TEST_CONST.TEST_CFG_FILE_PATH_LOGS,
            self.LOG_TOPIC_NAME,
            consumer_program_id,
        )
        self.assertTrue(len(consumer_log_msgs) > 0)
        for msg in consumer_log_msgs:
            msg_dict = json.loads(msg.value())
            self.assertTrue(float(msg_dict["timestamp"]) >= start_time)
        self.success = True  # pylint: disable=attribute-defined-outside-init

    def test_filepath_should_be_uploaded(self):
        """
        Test the function that says whether or not a file should be uploaded
        """
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
        except Exception as exc:
            raise exc
        finally:
            shutil.rmtree(subdir_path)
        self.success = True  # pylint: disable=attribute-defined-outside-init
