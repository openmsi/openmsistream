# imports
import pathlib, time, filecmp, os, configparser
from openmsistream.kafka_wrapper import OpenMSIStreamConsumer
from openmsistream.data_file_io.utilities import (
    get_encrypted_message_key_and_value_filenames,
)
from openmsistream.tools.undecryptable_messages.reproduce_undecryptable_messages import (
    main,
)

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


class TestUndecryptableMessages(
    TestWithKafkaTopics,
    TestWithDataFileUploadDirectory,
    TestWithDataFileDownloadDirectory,
):
    """
    Test behavior of messages that fail to be decrypted
    """

    TOPIC_NAME = "test_oms_undecryptable_messages"

    TOPICS = {
        TOPIC_NAME: {},
        f"{TOPIC_NAME}.keys": {"--partitions": 1},
        f"{TOPIC_NAME}.reqs": {"--partitions": 1},
        f"{TOPIC_NAME}.subs": {"--partitions": 1},
    }

    def test_undecryptable_messages_kafka(self):
        """
        Test the tool to re-produce encrypted messages
        """
        # create the upload directory
        self.create_upload_directory(cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_ENC)
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
        # write out the encrypted message keys/values to the encrypted messages directory
        enc_msgs_dir = self.output_dir / self.RECO_DIR_NAME / "ENCRYPTED_MESSAGES"
        consumer_group_id = f"test_undecrypted_messages_{TEST_CONST.PY_VERSION}"
        self.__write_out_encrypted_messages(enc_msgs_dir, consumer_group_id)
        # start up a download directory again, wait a bit, and make sure the file
        # hasn't come through yet (the other consumer should have used the same group id)
        self.create_download_directory(
            cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_ENC_2,
            topic_name=self.TOPIC_NAME,
            consumer_group_id=consumer_group_id,
        )
        self.start_download_thread()
        time.sleep(10)
        self.assertFalse((self.reco_dir / test_rel_filepath).is_file())
        # run the tool to reproduce the encrypted messages
        tool_config_file_path = self.__write_tool_config_file()
        tool_args = [str(arg) for arg in [tool_config_file_path, enc_msgs_dir]]
        try:
            main(tool_args)
            # make sure the file was reconstructed correctly from the re-produced messages
            # wait for the timeout for the test file to be completely reconstructed
            self.wait_for_files_to_reconstruct(test_rel_filepath, timeout_secs=300)
            # shut down the Producer
            self.stop_upload_thread()
            # make sure the reconstructed file exists same name as the original
            reco_fp = self.reco_dir / test_rel_filepath
            self.assertTrue(reco_fp.is_file())
            if not filecmp.cmp(TEST_CONST.TEST_DATA_FILE_PATH, reco_fp, shallow=False):
                errmsg = (
                    "ERROR: files are not the same after reconstruction! "
                    "(This may also be due to the timeout being too short)"
                )
                raise RuntimeError(errmsg)
        except Exception as exc:
            raise exc
        self.success = True  # pylint: disable=attribute-defined-outside-init

    def __write_out_encrypted_messages(self, enc_msgs_dir, consumer_group_id):
        """Use a regular consumer (with the given group ID) to get the encrypted
        messages' binary keys and values, and write those out to the given directory
        """
        if not enc_msgs_dir.is_dir():
            enc_msgs_dir.mkdir(parents=True)
        consumer_args, consumer_kwargs = OpenMSIStreamConsumer.get_consumer_args_kwargs(
            TEST_CONST.TEST_CFG_FILE_PATH
        )
        configs = consumer_args[1]
        configs.pop("key.deserializer")
        configs.pop("value.deserializer")
        configs["group.id"] = consumer_group_id
        adj_consumer_args = [consumer_args[0], configs]
        if len(consumer_args) > 2:
            adj_consumer_args += consumer_args[2:]
        consumer = OpenMSIStreamConsumer(*consumer_args, **consumer_kwargs)
        consumer.subscribe([self.TOPIC_NAME])
        n_msgs_consumed = 0
        while n_msgs_consumed < 12:
            msg = consumer.get_next_message()
            key_fn, value_fn = get_encrypted_message_key_and_value_filenames(
                msg, self.TOPIC_NAME
            )
            key_fp = enc_msgs_dir / key_fn
            value_fp = enc_msgs_dir / value_fn
            with open(key_fp, "wb") as fp:
                fp.write(bytes(msg.key()))
            self.assertTrue(key_fp.is_file())
            with open(value_fp, "wb") as fp:
                fp.write(bytes(msg.value()))
            self.assertTrue(value_fp.is_file())
            n_msgs_consumed += 1
            consumer.commit(message=msg, asynchronous=False)
        consumer.close()

    def __write_tool_config_file(self):
        """Write out a config file for the standalone encrypted message reproducer tool
        and return the path to it
        """
        tool_config_file_dir = self.output_dir / "cfg_files"
        if not tool_config_file_dir.is_dir():
            tool_config_file_dir.mkdir(parents=True)
        tool_node_id = "undecryptable-messages-reproducer"
        tool_config_file_name = f"{tool_node_id}.config"
        tool_config_file_path = tool_config_file_dir / tool_config_file_name
        tool_configs = configparser.ConfigParser(delimiters=[":"])
        tool_configs["DEFAULT"] = {"node_id": tool_node_id}
        if os.environ.get("LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS") and os.environ.get(
            "USE_LOCAL_KAFKA_BROKER_IN_TESTS"
        ):
            tool_configs[f"{tool_node_id}-kafka"] = {
                "bootstrap_servers": os.path.expandvars(
                    "$LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS"
                ),
            }
        else:
            tool_configs[f"{tool_node_id}-kafka"] = {
                "bootstrap_servers": os.path.expandvars(
                    "$KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS"
                ),
                "sasl_mechanism": "PLAIN",
                "security_protocol": "SASL_SSL",
                "sasl_username": os.path.expandvars("$KAFKA_TEST_CLUSTER_USERNAME"),
                "sasl_password": os.path.expandvars("$KAFKA_TEST_CLUSTER_PASSWORD"),
            }
        with open(tool_config_file_path, "w") as tool_config_file:
            tool_configs.write(tool_config_file)
        self.assertTrue(tool_config_file_path.is_file())
        return tool_config_file_path
