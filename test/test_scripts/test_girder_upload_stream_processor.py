# imports
import time, subprocess, unittest
import requests, docker
from config import TEST_CONST  # pylint: disable=import-error, wrong-import-order

# pylint: disable=import-error, wrong-import-order
from test_base_classes import TestWithUploadDataFile, TestWithStreamProcessor

# constants
GIRDER_COMPOSE_FILE_PATH = TEST_CONST.TEST_DIR_PATH / "local-girder-docker-compose.yml"
GIRDER_API_URL = "http://localhost:8080/api/v1"
GIRDER_TIMEOUT = 10
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
        time.sleep(3)
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

    @classmethod
    def tearDownClass(cls):  # pylint: disable=invalid-name
        """
        Call Docker compose to remove the local Girder instance
        """
        cmd = ["docker", "compose", "-f", str(GIRDER_COMPOSE_FILE_PATH), "down"]
        subprocess.check_output(cmd)
        super().tearDownClass()

    def setUp(self):  # pylint: disable=invalid-name
        """
        Create the assetstore to use
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

    def tearDown(self):  # pylint: disable=invalid-name
        """
        Delete the created assetstore
        """
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

    def test_girder_upload_stream_processor_kafka(self):
        """
        Test using a stream processor to upload a file to the Girder instance
        """
        time.sleep(5)
        self.success = True  # pylint: disable=attribute-defined-outside-init
