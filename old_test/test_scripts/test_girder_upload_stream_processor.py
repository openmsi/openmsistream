# imports
import json
import pathlib
import subprocess
import tempfile
import time
import unittest
from hashlib import sha512

import girder_client
import requests
import responses

try:
    from .config import TEST_CONST  # pylint: disable=import-error, wrong-import-order

    # pylint: disable=import-error, wrong-import-order
    from .base_classes import (
        TestWithKafkaTopics,
        TestWithStreamProcessor,
        TestWithUploadDataFile,
    )
except ImportError:
    from config import TEST_CONST  # pylint: disable=import-error, wrong-import-order

    # pylint: disable=import-error, wrong-import-order
    from base_classes import (
        TestWithKafkaTopics,
        TestWithStreamProcessor,
        TestWithUploadDataFile,
    )

import docker
from openmsistream import GirderUploadStreamProcessor

# constants
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
class TestGirderUploadStreamProcessor(
    TestWithKafkaTopics, TestWithUploadDataFile, TestWithStreamProcessor
):
    """
    Test using a stream processor to upload a file to a local Girder instance
    """

    TOPIC_NAME = "test_girder_upload_stream_processor"

    TOPICS = {TOPIC_NAME: {}}

    HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}

    assetstore_id = None
    api_key = None
    api_key_id = None

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
        counter = 0
        while counter < 30:
            try:
                resp = requests.get(GIRDER_API_URL, timeout=GIRDER_TIMEOUT)
                if resp.status_code == 200:
                    break
            except requests.exceptions.ConnectionError:
                pass
            time.sleep(1)
            counter += 1
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

        # Give girder time to start
        # Create the assetstore
        resp = requests.post(
            f"{GIRDER_API_URL}/assetstore",
            headers=cls.HEADERS,
            params={
                "type": 0,
                "name": "Base",
                # Girder container no longer running as root, use different folder
                # for assetstore root
                "root": "/home/girder/data/base",  # /srv/data/base
            },
            timeout=GIRDER_TIMEOUT,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                (
                    f"ERROR: failed to create Girder assetstore! Status: "
                    f"{resp.status_code} Text: {resp.text}"
                )
            )
        # Save the assetstore ID so we can delete it after the test
        cls.assetstore_id = resp.json()["_id"]
        # Create an API key
        resp = requests.post(
            f"{GIRDER_API_URL}/api_key",
            headers=cls.HEADERS,
            timeout=GIRDER_TIMEOUT,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"ERROR: Failed to create a Girder API key! Status: {resp.status_code}"
            )
        # Save the key and its ID so we can delete it after the test
        cls.api_key = resp.json()["key"]
        cls.api_key_id = resp.json()["_id"]

    def _produce_single_file(self, filepath):
        """
        Produce a single file to the Kafka topic
        """
        self.upload_single_file(
            filepath,
            topic_name=self.TOPIC_NAME,
            rootdir=TEST_CONST.TEST_DATA_DIR_PATH,
        )
        self.create_stream_processor(
            stream_processor_type=GirderUploadStreamProcessor,
            topic_name=self.TOPIC_NAME,
            consumer_group_id=f"test_girder_upload_stream_processor_{TEST_CONST.PY_VERSION}",
            other_init_args=(
                GIRDER_API_URL,
                self.api_key,
            ),
            other_init_kwargs={
                "collection_name": COLLECTION_NAME,
                "metadata": json.dumps({"somekey": "somevalue"}),
            },
        )
        self.start_stream_processor_thread()

    def test_girder_mimetype(self):
        """
        Test if Girder stream processor sets a proper mimeType
        """
        with tempfile.NamedTemporaryFile(
            dir=TEST_CONST.TEST_DATA_DIR_PATH, suffix=".png"
        ) as mock_png:
            mock_png.write(b"Pretend to be a PNG")
            mock_png.flush()
            png_path = pathlib.Path(mock_png.name)

            self._produce_single_file(png_path)
            try:
                rel_filepath = png_path.relative_to(TEST_CONST.TEST_DATA_DIR_PATH)
                self.wait_for_files_to_be_processed(rel_filepath, timeout_secs=180)

                girder = girder_client.GirderClient(apiUrl=GIRDER_API_URL)
                girder.authenticate(apiKey=self.api_key)

                gpath = (
                    f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/"
                    f"{self.TOPIC_NAME}/{rel_filepath.name}"
                )
                item = girder.get(
                    "resource/lookup",
                    parameters={"path": gpath},
                )
                if not item or item["_modelType"] != "item":
                    raise RuntimeError(f"ERROR: Item {rel_filepath.name} not found")
                fobj = next(girder.listFile(item["_id"]), None)
                if not fobj or fobj["_modelType"] != "file":
                    raise RuntimeError(f"ERROR: File {rel_filepath.name} not found")
                self.assertEqual(fobj["mimeType"], "image/png")
                girder.delete(f"/item/{item['_id']}")
            except Exception as exc:
                raise exc
        self.success = True  # pylint: disable=attribute-defined-outside-init

    @responses.activate
    def test_girder_handle_timeout(self):
        """
        Test if Girder stream processor handles server errors
        """
        # Allow the Girder API to be called
        responses.add_passthru(f"{GIRDER_API_URL}")
        # First call to /file should return 502 though
        responses.add(
            responses.POST,
            f"{GIRDER_API_URL}/file",
            status=502,
        )
        # but work after retry
        responses.add(
            responses.PassthroughResponse(responses.POST, f"{GIRDER_API_URL}/file")
        )

        self._produce_single_file(TEST_CONST.TEST_DATA_FILE_2_PATH)
        rel_filepath = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(
            TEST_CONST.TEST_DATA_DIR_PATH
        )
        self.wait_for_files_to_be_processed(rel_filepath, timeout_secs=180)
        self.success = True  # pylint: disable=attribute-defined-outside-init

    def test_girder_repeated_upload(self):
        """
        Test if Girder stream processor can handle an attempt to re-upload the same file
        """
        # wait until the file has been processed
        for _ in range(2):
            self._produce_single_file(TEST_CONST.TEST_DATA_FILE_2_PATH)
            rel_filepath = TEST_CONST.TEST_DATA_FILE_2_PATH.relative_to(
                TEST_CONST.TEST_DATA_DIR_PATH
            )
            self.wait_for_files_to_be_processed(rel_filepath, timeout_secs=180)
            # simulate re-runing the stream processor
            self.reset_stream_processor()

        girder = girder_client.GirderClient(apiUrl=GIRDER_API_URL)
        girder.authenticate(apiKey=self.api_key)
        gpath = (
            f"/collection/{COLLECTION_NAME}/{COLLECTION_NAME}/"
            f"{self.TOPIC_NAME}/{rel_filepath.name} (1)"
        )
        item = girder.get(
            "/resource/lookup",
            parameters={"path": gpath, "test": True},
        )
        self.assertIsNone(item, "Item should not exist")
        self.success = True  # pylint: disable=attribute-defined-outside-init

    def test_girder_upload_stream_processor_kafka(self):
        """
        Test using a stream processor to upload a file to the Girder instance
        """
        self._produce_single_file(TEST_CONST.TEST_DATA_FILE_2_PATH)
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
                if resp["_modelType"] == "folder" and resp["name"] == self.TOPIC_NAME:
                    topic_folder_id = resp["_id"]
            if not topic_folder_id:
                errmsg = (
                    f"ERROR: could not find the '{self.TOPIC_NAME}' folder "
                    f"in the collection folder! Responses: {list(resps)}"
                )
                raise RuntimeError(errmsg)
            resps = girder.listItem(topic_folder_id, name=rel_filepath.name)
            item_id = None
            for resp in resps:
                self.assertEqual(resp["meta"].get("somekey"), "somevalue")
                item_id = resp["_id"]
            if not item_id:
                raise RuntimeError(f"Couldn't find Item! Responses: {list(resps)}")
            resps = girder.listFile(item_id)
            for resp in resps:
                file_id = resp["_id"]
            if not file_id:
                raise RuntimeError(f"Couldn't find File! Responses: {list(resps)}")
            file_stream = girder.downloadFileAsIterator(file_id)
            girder_hash = sha512()
            for chunk in file_stream:
                girder_hash.update(chunk)
            girder_hash = girder_hash.digest()
            self.assertEqual(original_hash, girder_hash)
            girder.delete(f"/item/{item_id}")
        except Exception as exc:
            raise exc
        self.success = True  # pylint: disable=attribute-defined-outside-init

    @classmethod
    def tearDownClass(cls):  # pylint: disable=invalid-name
        """
        Call Docker compose to remove the local Girder instance
        """
        cmd = [
            "docker",
            "compose",
            "-f",
            str(GIRDER_COMPOSE_FILE_PATH),
            "down",
            "-t",
            "0",
        ]
        subprocess.check_output(cmd)
        super().tearDownClass()
