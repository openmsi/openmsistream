# imports
import pathlib, time, subprocess, unittest
from hashlib import sha512
import requests, docker, girder_client  # pylint: disable=wrong-import-order
from openmsistream.girder import GirderUploadStreamProcessor
from config import TEST_CONST  # pylint: disable=import-error, wrong-import-order

# pylint: disable=import-error, wrong-import-order
from test_base_classes import TestWithUploadDataFile, TestWithStreamProcessor

# constants
TOPIC_NAME = TEST_CONST.TEST_TOPIC_NAMES[pathlib.Path(__file__).name[: -len(".py")]]
GIRDER_COMPOSE_FILE_PATH = TEST_CONST.TEST_DIR_PATH / "local-girder-docker-compose.yml"
GIRDER_API_URL = "http://localhost:8080/api/v1"
GIRDER_TIMEOUT = 10
COLLECTION_NAME = "testing_collection"
DOCKER_RUNNING = True
try:
    client = docker.from_env()
except docker.errors.DockerException:
    DOCKER_RUNNING = False


@unittest.skipUnless(
    DOCKER_RUNNING,
    "Docker must be running to test Girder upload (uses local Girder in Docker Compose)",
)
class TestGirderUploadStreamProcessor(TestWithUploadDataFile, TestWithStreamProcessor):
    """
    Test using a stream processor to upload a file to a local Girder instance
    """

    HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.assetstore_id = None
        self.api_key = None
        self.api_key_id = None
        self.file_id = None

    @classmethod
    def setUpClass(cls):  # pylint: disable=invalid-name
        """
        Call Docker compose to start the local Girder instance, and create the admin user
        """
        super().setUpClass()
        # start up the local Girder compose
        cmd = ["docker", "compose", "-f", str(GIRDER_COMPOSE_FILE_PATH), "up", "-d"]
        subprocess.check_output(cmd)
        # wait for it to be available
        time.sleep(5)
        # create the admin user
        resp = requests.post(
            f"{GIRDER_API_URL}/user",
            headers=cls.HEADERS,
            params={
                "login": "admin",
                "email": "root@dev.null",
                "firstName": "John",
                "lastName": "Doe",
                "password": "arglebargle123",
                "admin": True,
            },
            timeout=GIRDER_TIMEOUT,
        )
        if resp.status_code == 400:
            raise RuntimeError(
                "Admin Girder user already exists. Database was not purged."
            )
        # Store token for future requests
        cls.HEADERS["Girder-Token"] = resp.json()["authToken"]["token"]

    def setUp(self):  # pylint: disable=invalid-name
        """
        Create the assetstore to use, and an API key
        """
        super().setUp()
        # Give girder time to start
        self.logger.info("Waiting for Girder to start")
        while True:
            resp = requests.get(GIRDER_API_URL, timeout=GIRDER_TIMEOUT)
            if resp.status_code == 200:
                break
            time.sleep(1)
        # Create the assetstore
        self.logger.info("Creating default assetstore")
        resp = requests.post(
            f"{GIRDER_API_URL}/assetstore",
            headers=self.HEADERS,
            params={
                "type": 0,
                "name": "Base",
                "root": "/srv/data/base",
            },
            timeout=GIRDER_TIMEOUT,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"ERROR: failed to create Girder assetstore! Status: {resp.status_code}"
            )
        # Save the assetstore ID so we can delete it after the test
        self.assetstore_id = resp.json()["_id"]
        # Create an API key
        resp = requests.post(
            f"{GIRDER_API_URL}/api_key",
            headers=self.HEADERS,
            timeout=GIRDER_TIMEOUT,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"ERROR: Failed to create a Girder API key! Status: {resp.status_code}"
            )
        # Save the key and its ID so we can delete it after the test
        self.api_key = resp.json()["key"]
        self.api_key_id = resp.json()["_id"]

    def test_girder_upload_stream_processor_kafka(self):
        """
        Test using a stream processor to upload a file to the Girder instance
        """
        # upload the test file
        self.upload_single_file(
            TEST_CONST.TEST_DATA_FILE_2_PATH,
            topic_name=TOPIC_NAME,
            rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
        )
        # start up a stream processor to read its data back into memory
        self.create_stream_processor(
            stream_processor_type=GirderUploadStreamProcessor,
            topic_name=TOPIC_NAME,
            consumer_group_id=f"test_girder_upload_stream_processor_{TEST_CONST.PY_VERSION}",
            other_init_args=(
                GIRDER_API_URL,
                self.api_key,
            ),
            other_init_kwargs={
                "collection_name": COLLECTION_NAME,
                "provider": "test_provider",
            },
        )
        self.start_stream_processor_thread()
        try:
            # wait until the file has been processed
            rel_filepath = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(
                TEST_CONST.TEST_DATA_DIR_PATH
            )
            self.wait_for_files_to_be_processed(rel_filepath, timeout_secs=180)
            # make sure the contents of the file are the same as the original
            original_hash = sha512()
            with open(TEST_CONST.TEST_DATA_FILE_2_PATH, "rb") as fp:
                original_hash.update(fp.read())
            original_hash = original_hash.digest()
            girder = girder_client.GirderClient(apiUrl=GIRDER_API_URL)
            girder.authenticate(apiKey=self.api_key)
            coll_id = None
            resps = girder.listCollection()
            for resp in resps:
                if resp["_modelType"] == "collection" and resp["name"] == COLLECTION_NAME:
                    coll_id = resp["_id"]
            if not coll_id:
                errmsg = (
                    f"ERROR: could not find the '{COLLECTION_NAME}' collection! "
                    f"Responses: {list(resps)}"
                )
                raise RuntimeError(errmsg)
            coll_folder_id = None
            resps = girder.listFolder(coll_id, parentFolderType="collection")
            for resp in resps:
                if resp["_modelType"] == "folder" and resp["name"] == COLLECTION_NAME:
                    coll_folder_id = resp["_id"]
            if not coll_folder_id:
                errmsg = (
                    f"ERROR: could not find the '{COLLECTION_NAME}' folder "
                    f"in the collection! Responses: {list(resps)}"
                )
                raise RuntimeError(errmsg)
            topic_folder_id = None
            resps = girder.listFolder(coll_folder_id)
            for resp in resps:
                if resp["_modelType"] == "folder" and resp["name"] == TOPIC_NAME:
                    topic_folder_id = resp["_id"]
            if not topic_folder_id:
                errmsg = (
                    f"ERROR: could not find the '{TOPIC_NAME}' folder "
                    f"in the collection folder! Responses: {list(resps)}"
                )
                raise RuntimeError(errmsg)
            resps = girder.listItem(topic_folder_id)
            item_id = None
            for resp in resps:
                item_id = resp["_id"]
            if not item_id:
                raise RuntimeError(f"Couldn't find Item! Responses: {list(resps)}")
            resps = girder.listFile(item_id)
            for resp in resps:
                self.file_id = resp["_id"]
            if not self.file_id:
                raise RuntimeError(f"Couldn't find File! Responses: {list(resps)}")
            file_stream = girder.downloadFileAsIterator(self.file_id)
            girder_hash = sha512()
            for chunk in file_stream:
                girder_hash.update(chunk)
            girder_hash = girder_hash.digest()
            self.assertEqual(original_hash, girder_hash)
        except Exception as exc:
            raise exc
        self.success = True  # pylint: disable=attribute-defined-outside-init

    def tearDown(self):  # pylint: disable=invalid-name
        """
        Delete the created API key and the file/assetstore
        """
        self.logger.info("Deleting API key")
        resp = requests.delete(
            f"{GIRDER_API_URL}/api_key/{self.api_key_id}",
            headers=self.HEADERS,
            timeout=GIRDER_TIMEOUT,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"ERROR: Failed to delete Girder API key! Status: {resp.status_code}"
            )
        self.logger.info("Deleting file")
        resp = requests.delete(
            f"{GIRDER_API_URL}/file/{self.file_id}",
            headers=self.HEADERS,
            timeout=GIRDER_TIMEOUT,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"ERROR: Failed to delete file! Status: {resp.status_code}"
            )
        self.logger.info("Deleting assetstore")
        resp = requests.delete(
            f"{GIRDER_API_URL}/assetstore/{self.assetstore_id}",
            headers=self.HEADERS,
            timeout=GIRDER_TIMEOUT,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"ERROR: Failed to delete assetstore! Status: {resp.status_code}"
            )
        super().tearDown()

    @classmethod
    def tearDownClass(cls):  # pylint: disable=invalid-name
        """
        Call Docker compose to remove the local Girder instance
        """
        cmd = ["docker", "compose", "-f", str(GIRDER_COMPOSE_FILE_PATH), "down"]
        subprocess.check_output(cmd)
        super().tearDownClass()
