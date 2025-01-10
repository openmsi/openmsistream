# imports
import os, pathlib, sys
from openmsistream.utilities.config import RUN_CONST


class TestRoutineConstants:
    """
    constants used in running tests
    """

    # Paths to locations inside the code base
    TEST_DIR_PATH = (pathlib.Path(__file__).parent.parent).resolve()
    EXAMPLES_DIR_PATH = TEST_DIR_PATH.parent / "examples"
    TEST_DATA_DIR_PATH = TEST_DIR_PATH / "data"

    # S3 connection information
    # the endpoint_url for s3 bucket connection
    TEST_ENDPOINT_URL = (
        os.environ["ENDPOINT_URL"] if "ENDPOINT_URL" in os.environ else "ENDPOINT_URL"
    )
    # the bucket name to upload data to (S3Client)
    TEST_BUCKET_NAME = (
        os.environ["BUCKET_NAME"] if "BUCKET_NAME" in os.environ else "BUCKET_NAME"
    )
    # the access_key_id for s3 Authentication
    TEST_ACCESS_KEY_ID = (
        os.environ["ACCESS_KEY_ID"] if "ACCESS_KEY_ID" in os.environ else "ACCESS_KEY_ID"
    )
    # the secret_key_id for s3 Authentication
    TEST_SECRET_KEY_ID = (
        os.environ["SECRET_KEY_ID"] if "SECRET_KEY_ID" in os.environ else "SECRET_KEY_ID"
    )
    # the region for the testing s3 bucket
    TEST_REGION = os.environ["REGION"] if "REGION" in os.environ else "REGION"

    # different config files used in tests
    TEST_CFG_FILE_PATH = (
        RUN_CONST.CONFIG_FILE_DIR
        / f"{RUN_CONST.DEFAULT_CONFIG_FILE}{RUN_CONST.CONFIG_FILE_EXT}"
    )
    # Same as above except it includes a node_id to test encryption
    TEST_CFG_FILE_PATH_ENC = TEST_CFG_FILE_PATH.with_name(
        f"test_encrypted{TEST_CFG_FILE_PATH.suffix}"
    )
    TEST_CFG_FILE_PATH_ENC_2 = TEST_CFG_FILE_PATH.with_name(
        f"test_encrypted_2{TEST_CFG_FILE_PATH.suffix}"
    )
    # Same as above except it includes S3 transfer configs
    TEST_CFG_FILE_PATH_S3 = TEST_CFG_FILE_PATH.with_name(
        f"test_s3_transfer{TEST_CFG_FILE_PATH.suffix}"
    )
    # the config file to use for the final consumer in the metadata reproducer test
    TEST_CFG_FILE_PATH_MDC = TEST_CFG_FILE_PATH.with_name(
        f"test_metadata_rep_consumer{TEST_CFG_FILE_PATH.suffix}"
    )
    # test heartbeats
    TEST_CFG_FILE_PATH_HEARTBEATS = TEST_CFG_FILE_PATH.with_name(
        f"test_heartbeats{TEST_CFG_FILE_PATH.suffix}"
    )
    TEST_CFG_FILE_PATH_HEARTBEATS_ENC = TEST_CFG_FILE_PATH.with_name(
        f"test_heartbeats_enc{TEST_CFG_FILE_PATH.suffix}"
    )
    TEST_CFG_FILE_PATH_HEARTBEATS_ENC_2 = TEST_CFG_FILE_PATH.with_name(
        f"test_heartbeats_enc_2{TEST_CFG_FILE_PATH.suffix}"
    )
    # test logs
    TEST_CFG_FILE_PATH_LOGS = TEST_CFG_FILE_PATH.with_name(
        f"test_logs{TEST_CFG_FILE_PATH.suffix}"
    )
    TEST_CFG_FILE_PATH_LOGS_ENC = TEST_CFG_FILE_PATH.with_name(
        f"test_logs_enc{TEST_CFG_FILE_PATH.suffix}"
    )
    TEST_CFG_FILE_PATH_LOGS_ENC_2 = TEST_CFG_FILE_PATH.with_name(
        f"test_logs_enc_2{TEST_CFG_FILE_PATH.suffix}"
    )

    # If an environment variable indicates that a local broker is being used,
    # prepend "local_broker_" to the names of the above config files used in tests
    if os.environ.get("LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS") and os.environ.get(
        "USE_LOCAL_KAFKA_BROKER_IN_TESTS"
    ):
        TEST_CFG_FILE_PATH = TEST_CFG_FILE_PATH.with_name(
            f"local_broker_{TEST_CFG_FILE_PATH.name}"
        )
        TEST_CFG_FILE_PATH_ENC = TEST_CFG_FILE_PATH_ENC.with_name(
            f"local_broker_{TEST_CFG_FILE_PATH_ENC.name}"
        )
        TEST_CFG_FILE_PATH_ENC_2 = TEST_CFG_FILE_PATH_ENC_2.with_name(
            f"local_broker_{TEST_CFG_FILE_PATH_ENC_2.name}"
        )
        TEST_CFG_FILE_PATH_S3 = TEST_CFG_FILE_PATH_S3.with_name(
            f"local_broker_{TEST_CFG_FILE_PATH_S3.name}"
        )
        TEST_CFG_FILE_PATH_MDC = TEST_CFG_FILE_PATH_MDC.with_name(
            f"local_broker_{TEST_CFG_FILE_PATH_MDC.name}"
        )
        TEST_CFG_FILE_PATH_HEARTBEATS = TEST_CFG_FILE_PATH_HEARTBEATS.with_name(
            f"local_broker_{TEST_CFG_FILE_PATH_HEARTBEATS.name}"
        )
        TEST_CFG_FILE_PATH_HEARTBEATS_ENC = TEST_CFG_FILE_PATH_HEARTBEATS.with_name(
            f"local_broker_{TEST_CFG_FILE_PATH_HEARTBEATS_ENC.name}"
        )
        TEST_CFG_FILE_PATH_HEARTBEATS_ENC_2 = TEST_CFG_FILE_PATH_HEARTBEATS.with_name(
            f"local_broker_{TEST_CFG_FILE_PATH_HEARTBEATS_ENC_2.name}"
        )
        TEST_CFG_FILE_PATH_LOGS = TEST_CFG_FILE_PATH_LOGS.with_name(
            f"local_broker_{TEST_CFG_FILE_PATH_LOGS.name}"
        )
        TEST_CFG_FILE_PATH_LOGS_ENC = TEST_CFG_FILE_PATH_LOGS.with_name(
            f"local_broker_{TEST_CFG_FILE_PATH_LOGS_ENC.name}"
        )
        TEST_CFG_FILE_PATH_LOGS_ENC_2 = TEST_CFG_FILE_PATH_LOGS.with_name(
            f"local_broker_{TEST_CFG_FILE_PATH_LOGS_ENC_2.name}"
        )
    # The path to the "prod" Kafka config file to use
    PROD_CONFIG_FILE_PATH = TEST_CFG_FILE_PATH.with_name(
        f"prod{TEST_CFG_FILE_PATH.suffix}"
    )
    FAKE_PROD_CONFIG_FILE_PATH = (
        TEST_DATA_DIR_PATH / f"fake_prod{RUN_CONST.CONFIG_FILE_EXT}"
    )

    # Names of and paths to directories and files used in testing
    TEST_DATA_FILE_ROOT_DIR_NAME = "test_file_root_dir"
    TEST_DATA_FILE_SUB_DIR_NAME = "test_file_sub_dir"
    TEST_DATA_FILE_NAME = "1a0ceb89-b5f0-45dc-9c12-63d3020e2217.dat"
    TEST_DATA_FILE_2_NAME = "4ceee043-0b99-4f49-8527-595d93ddc487.dat"
    TEST_DATA_FILE_ROOT_DIR_PATH = TEST_DATA_DIR_PATH / TEST_DATA_FILE_ROOT_DIR_NAME
    TEST_DATA_FILE_PATH = (
        TEST_DATA_DIR_PATH
        / TEST_DATA_FILE_ROOT_DIR_NAME
        / TEST_DATA_FILE_SUB_DIR_NAME
        / TEST_DATA_FILE_NAME
    )
    TEST_DATA_FILE_2_PATH = TEST_DATA_DIR_PATH / TEST_DATA_FILE_2_NAME
    TEST_METADATA_DICT_PICKLE_FILE = TEST_DATA_DIR_PATH / "test_metadata_dict.pickle"

    # size (in bytes) of chunks to use in tests
    TEST_CHUNK_SIZE = 16384

    # URL to download the example metadata extraction/tutorial plots file from
    TUTORIAL_TEST_FILE_URL = "https://data.paradim.org/194/XRD/SC001/SC001%20XRR.csv"

    # Names of environment variables that need to be edited to run tests with a local broker
    ENV_VAR_NAMES = [
        "KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS",
        "KAFKA_TEST_CLUSTER_USERNAME",
        "KAFKA_TEST_CLUSTER_PASSWORD",
    ]

    # Version tag to use for separating output locations, consumer group IDs, etc.
    # for concurrently-running tests
    PY_VERSION = f"python_{sys.version.split()[0].replace('.','_')}"


TEST_CONST = TestRoutineConstants()
