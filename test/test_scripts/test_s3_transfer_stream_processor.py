# imports
import hashlib
import pathlib
import sys
from openmsistream import S3TransferStreamProcessor, DataFileUploadDirectory
from openmsistream.s3_buckets.s3_data_transfer import S3DataTransfer

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


class TestS3TransferStreamProcessor(
    TestWithKafkaTopics, TestWithDataFileUploadDirectory, TestWithStreamProcessor
):
    """
    Class for testing S3TransferStreamProcessor
    """

    TOPIC_NAME = "test_s3_transfer_stream_processor"

    TOPICS = {TOPIC_NAME: {}}

    def setUp(self):
        """
        Set up the test
        """
        super().setUp()
        prefix = f"py{sys.version_info.major}{sys.version_info.minor}-"
        fname = prefix + TEST_CONST.TEST_DATA_FILE_NAME
        self.rel_filepath = pathlib.Path(TEST_CONST.TEST_DATA_FILE_SUB_DIR_NAME) / fname

    def run_data_file_upload_directory(self):
        """
        Called by the test method below to upload a test file
        """
        # make the directory to watch
        self.create_upload_directory(cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_S3)
        # start upload_files_as_added in a separate thread so we can time it out
        self.start_upload_thread(self.TOPIC_NAME)
        try:
            # copy the test file into the watched directory
            self.copy_file_to_watched_dir(
                TEST_CONST.TEST_DATA_FILE_PATH, self.rel_filepath
            )
            # stop the upload thread
            self.stop_upload_thread()
        except Exception as exc:
            raise exc

    def run_s3_tranfer_data(self):
        """
        Called by the test method below to transfer reconstructed files to the S3 bucket
        """
        # create and start up the stream processor
        self.create_stream_processor(
            S3TransferStreamProcessor,
            cfg_file=TEST_CONST.TEST_CFG_FILE_PATH_S3,
            topic_name=self.TOPIC_NAME,
            consumer_group_id=f"test_s3_transfer_{TEST_CONST.PY_VERSION}",
            other_init_args=(TEST_CONST.TEST_BUCKET_NAME,),
        )
        self.start_stream_processor_thread(self.stream_processor.make_stream)
        try:
            # wait for the test file to be processed
            self.wait_for_files_to_be_processed(self.rel_filepath, timeout_secs=300)
        except Exception as exc:
            raise exc

    def hash_file(self, my_file):
        """
        Return the md5 hash of a given file
        """
        md5 = hashlib.md5()
        with open(my_file, "rb") as fp:
            while True:
                data = fp.read(65536)
                if not data:
                    break
                md5.update(data)
        return format(md5.hexdigest())

    def validate_s3_transfer(self):
        """
        Make sure contents on disk match the contents in the bucket
        """
        endpoint_url = TEST_CONST.TEST_ENDPOINT_URL
        if not endpoint_url.startswith("https://"):
            endpoint_url = "https://" + endpoint_url
        s3_config = {
            "endpoint_url": endpoint_url,
            "access_key_id": TEST_CONST.TEST_ACCESS_KEY_ID,
            "secret_key_id": TEST_CONST.TEST_SECRET_KEY_ID,
            "region": TEST_CONST.TEST_REGION,
            "bucket_name": TEST_CONST.TEST_BUCKET_NAME,
        }
        s3d = S3DataTransfer(s3_config, logger=self.logger)
        log_subdir = self.watched_dir / DataFileUploadDirectory.LOG_SUBDIR_NAME
        for filepath in self.watched_dir.rglob("*"):
            if filepath.is_dir():
                continue
            try:
                if filepath.is_relative_to(log_subdir):
                    continue
            except AttributeError:  # "is_relative_to" was added after python 3.7
                if str(filepath).startswith(str(log_subdir)):
                    continue
            file_hash = self.hash_file(filepath)
            object_key = f"{self.TOPIC_NAME}/{filepath.relative_to(self.watched_dir)}"
            if not (
                s3d.compare_producer_datafile_with_s3_object_stream(
                    TEST_CONST.TEST_BUCKET_NAME, object_key, file_hash
                )
            ):
                raise ValueError(
                    "ERROR: Failed to match s3 object with the original data"
                )
            s3d.delete_object_from_bucket(TEST_CONST.TEST_BUCKET_NAME, object_key)

    def test_upload_and_transfer_into_s3_bucket_kafka(self):
        """
        Actually run the test
        """
        self.run_data_file_upload_directory()
        self.run_s3_tranfer_data()
        self.validate_s3_transfer()
        self.success = True  # pylint: disable=attribute-defined-outside-init
