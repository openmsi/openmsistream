#imports
import unittest, os

#constants
ENV_VAR_NAMES = [
    'KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS',
    'KAFKA_TEST_CLUSTER_USERNAME',
    'KAFKA_TEST_CLUSTER_PASSWORD'
]

class TestWithEnvVars(unittest.TestCase) :
    """
    Class for performing tests that don't interact with the Kafka cluster
    but that do need some environment variable values set to run properly
    """

    @classmethod
    def setUpClass(cls) :
        #set some placeholder environment variable values if they're not already on the system
        cls.ENV_VARS_TO_RESET = []
        for env_var_name in ENV_VAR_NAMES :
            if os.path.expandvars(f'${env_var_name}')==f'${env_var_name}' :
                os.environ[env_var_name]=f'PLACEHOLDER_{env_var_name}'
                cls.ENV_VARS_TO_RESET.append(env_var_name)

    @classmethod
    def tearDownClass(cls) :
        #unset any of the placeholder environment variables
        for env_var_name in cls.ENV_VARS_TO_RESET :
            os.environ.pop(env_var_name)