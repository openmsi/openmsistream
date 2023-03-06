==========
Code Tests
==========

There are several tests for the codebase already written (and more will be added over time). The repo also has a continuous integration workflow set up on `CircleCI <https://circleci.com/>`_, and the checks in that workflow must pass successfully on any branch being merged into main via a Pull Request.  

Running tests interactively 
---------------------------

If you're editing the code, you can make sure it doesn't break anything currently being tested by running::
    
    python test/run_all_tests.py
    
from just inside the directory of the repo. If you'd like to add more tests, you can include any classes that extend :class:`unittest.TestCase` in the `test/test_scripts subdirectory <https://github.com/openmsi/openmsistream/tree/main/test/test_scripts>`_. If you name their files anything that starts with "test", the ``run_all_tests.py`` script will run them automatically. 

If any new test methods interact with the Kafka broker, you should end their names with "kafka" so that ``run_all_tests.py`` can exclude them if requested. Running the tests also requires that ``pyflakes``, ``pylint``, and a few other dependencies be installed, which you can get right from this repo by running ``pip install openmsistream[test]`` (with or without the ``--editable`` or ``-e`` flag(s)).

There are also a few options you can add to ``run_all_tests.py`` if you only want to run some subset of the available tests:

#. Add the ``--no_pyflakes`` flag to skip the pyflakes test
#. Add the ``--no_pylint`` flag to skip the pylint checks
#. Add the ``--no_script_tests`` flag to skip the tests in "test_scripts" entirely, OR
#. Add the ``--no_kafka`` flag to skip running tests that need to communicate with the Kafka broker. Adding this flag automatically **skips any test methods whose names end with "kafka"**; you will see that they were skipped at the end of the output. Adding this flag also runs tests with some environment variables un-set to ensure that these tests are truly independent of communication with a Kafka broker.
#. Add the ``--test_regex [regex]`` option to skip any tests whose function names aren't matched to the ``[regex]`` regular expression
#. Add the ``--local_broker`` flag to run tests using a local Kafka broker running in Docker instead of a third party broker. Including this flag automatically `starts up <https://github.com/openmsi/openmsistream/blob/main/test/start_local_broker.sh#L5>`_ the broker and `creates the topics needed <https://github.com/openmsi/openmsistream/blob/main/test/create_local_testing_topics.sh#L5-L29>`_ before running the tests, and it `stops the broker <https://github.com/openmsi/openmsistream/blob/main/test/stop_local_broker.sh#L5>`_ running when the tests complete. Adding this flag assumes that Docker is installed and that the Docker daemon is running. 
#. Add the ``--failfast`` flag to stop executing the script early if any individual test(s) fail. Normally all tests are run regardless of how many fail, but including this flag will stop the run as soon as any test fails.

**NOTE:** If you are running the ``run_all_tests.py`` script interactively on a Linux system with ``systemd`` installed, some of the tests will temporarily install code as daemons. You should therefore activate sudo privileges before running the script, with::

    sudo ls

or similar.

CircleCI website
----------------

On the CircleCI website you can manually run tests on any branch you'd like. Tests will also automatically run for each commit on every branch, and must pass to merge any pull requests into the main branch. The configuration for CircleCI is in the `.circleci/config.yml file in the root directory of the repository <https://github.com/openmsi/openmsistream/blob/main/.circleci/config.yml>`_. 

Running tests successfully on CircleCI requires that the Project on CircleCI has environment variables for the test broker username and password registered within it, as well as environment variables to allow access to the IDIES/SciServer Docker registry, and environment variables to connect to the S3 bucket used for testing.

Rebuilding static test data
---------------------------

Some of the tests rely on static example data in `test/data <https://github.com/openmsi/openmsistream/tree/main/test/data>`_. If you need to regenerate these data under some new conditions (i.e., because you've changed default options someplace), you can run ``python test/rebuild_test_reference_data.py`` and follow the prompts it gives you to replace the necessary files. You can also add to that script if you write any new tests that rely on example data. Static test data should be committed to the repo like any other file, and they'll be picked up both interactively and on CircleCI.

External requirements for running tests
---------------------------------------

All tests can be run using a local Kafka broker by adding the ``--local_broker`` flag to the "run_all_tests" script. Alternatively, tests can use a remote Kafka broker whose bootstrap servers, username, and password are set as the ``KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS``, ``KAFKA_TEST_CLUSTER_USERNAME``, and ``KAFKA_TEST_CLUSTER_PASSWORD`` environment variables, respectively. The OpenMSI development group uses a Kafka broker cluster running on `Confluent Cloud <https://confluent.cloud/>`_, and if you get the necessary environment variable values from a developer in the group you won't need to do any extra work.

But, if you would like to run the CI tests using a different Kafka broker than the automatically-created local broker or the cluster set up by the OpenMSI development group, you will need to make several changes. First, you'll need access to a Kafka cluster. Options include a self-managed broker running standalone or in Docker (see Kafka documentation for these options), or a broker managed as a service on `the Confluent Cloud website <https://confluent.cloud/>`_.

Regardless of which broker solution you use, though, you'll next need to add an environment variable to your local system called ``KAFKA_TEST_CLUSTER_BOOTSTRAP_SERVERS`` that specifies how to connect to the broker. For a local broker you will configure this in setting it up, and for brokers on Confluent Cloud this value is the one shown under "Bootstrap server" on the page for your cluster (under "Cluster settings"). 

If you use Confluent Cloud, or if you add authentication to your self-managed broker, you'll also need to add environment variables called ``KAFKA_TEST_CLUSTER_USERNAME`` and ``KAFKA_TEST_CLUSTER_PASSWORD`` to hold the authentication username and password, respectively. For brokers on Confluent Cloud, you can add authentication by creating a new API key under "API keys" on the website, and set the environment variable values to the username and password of the api key.

Then, you'll need to create the topics used in running the tests. You can add topics using the Kafka CLI, or under the ``Topics`` section for your cluster on the Confluent Cloud website. The following topics should be created, ideally with 2 partitions each, and it's recommended to set the cleanup policy to "delete" with a max retention time of 1 hour to keep the topics mostly empty for testing purposes.

    #. ``test``
    #. ``test_data_file_directories``
    #. ``test_oms_encrypted``
    #. ``test_data_file_stream_processor``
    #. ``test_data_file_stream_processor_2``
    #. ``test_data_file_stream_processor_encrypted``
    #. ``test_s3_transfer_stream_processor``
    #. ``test_metadata_extractor_source``
    #. ``test_metadata_extractor_dest``
    #. ``test_plots_for_tutorial``

The ``test_oms_encrypted`` and ``test_data_file_stream_processor_encrypted`` topics hold messages encrypted with KafkaCrypto; those topics each need three additional "key-passing" topics called ``[topic_name].keys``, ``[topic_name].reqs``, and ``[topic_name].subs``. These additional topics can have only one partition each, and can use the "compact" cleanup policy (they will not end up storing a huge amount of data). 

Lastly, testing the S3 transfer function requires access to an S3 bucket on a system such as `AWS <https://aws.amazon.com/s3/>`_. Take note of the access key ID, secret key ID, endpoint URL, region (i.e. ``us-west-1``), and bucket name, and set those as environment variables on your local system called ``ACCESS_KEY_ID``, ``SECRET_KEY_ID``, ``ENDPOINT_URL``, ``REGION``, and ``BUCKET_NAME``. Without valid values for these environment variables, the test for the S3 transfer function will fail.
