# imports
import os
import pathlib
import re
import subprocess
import unittest
from argparse import ArgumentParser

import xmlrunner
from openmsitoolbox.logging import OpenMSILogger
from tempenv import TemporaryEnvironment

import docker
from openmsistream.services.utilities import run_cmd_in_subprocess

# pylint: disable=wrong-import-order,import-error
from test_scripts.config import TEST_CONST

# constants
TEST_DIR_PATH = pathlib.Path(__file__).resolve().parent
TOP_DIR_PATH = TEST_DIR_PATH.parent
START_LOCAL_BROKER_SCRIPT_PATH = TEST_DIR_PATH / "start_local_broker.sh"
STOP_LOCAL_BROKER_SCRIPT_PATH = TEST_DIR_PATH / "stop_local_broker.sh"
TEST_SCRIPT_DIR_PATH = TEST_DIR_PATH / "test_scripts"
TEST_REPO_STATUS_SCRIPT_PATH = TEST_DIR_PATH / "test_repo_status.sh"
CWD = pathlib.Path().resolve()
LOGGER = OpenMSILogger("run_all_tests")


def get_args(args):
    """
    Return the parsed command line arguments
    """
    parser = ArgumentParser()
    local_broker_group = parser.add_mutually_exclusive_group()
    local_broker_group.add_argument(
        "--setup_local_broker",
        action="store_true",
        help="Add this flag to set up the local broker and quit",
    )
    local_broker_group.add_argument(
        "--teardown_local_broker",
        action="store_true",
        help="Add this flag to tear down the local broker and quit",
    )
    parser.add_argument(
        "--no_pyflakes",
        action="store_true",
        help="Add this flag to skip running the pyflakes check",
    )
    parser.add_argument(
        "--no_formatting",
        action="store_true",
        help="Add this flag to skip running the formatting checks",
    )
    parser.add_argument(
        "--no_pylint",
        action="store_true",
        help="Add this flag to skip running the pylint checks",
    )
    script_test_opts = parser.add_mutually_exclusive_group()
    script_test_opts.add_argument(
        "--no_script_tests",
        action="store_true",
        help=f"Add this flag to skip running the tests in {TEST_SCRIPT_DIR_PATH.name}",
    )
    script_test_opts.add_argument(
        "--no_kafka",
        action="store_true",
        help=f"Add this flag to skip running the tests in {TEST_SCRIPT_DIR_PATH.name}",
    )
    script_test_opts.add_argument(
        "--test_regex",
        type=re.compile,
        default=None,
        help="Only tests whose function names match this regex will be run",
    )
    parser.add_argument(
        "--local_broker",
        action="store_true",
        help=(
            "Add this flag to automatically set up a local Kafka broker with Docker "
            "and use that broker to run tests instead of a third-party broker "
            "configured using environment variables"
        ),
    )
    parser.add_argument(
        "--skip_broker_teardown",
        action="store_true",
        help=(
            "Add this flag to skip tearing down the local Kafka broker "
            "after running tests. The local broker can be shut down manually "
            'by rerunning with "--teardown_local_broker".'
        ),
    )
    parser.add_argument(
        "--no_repo",
        action="store_true",
        help="Add this flag to skip running the Git repository checks",
    )
    parser.add_argument(
        "--failfast",
        action="store_true",
        help="Add this flag to exit after the first failed test",
    )
    return parser.parse_args(args=args)


def test_pyflakes(args):
    """
    If requested, run pyflakes and check the output for errors
    """
    if args.no_pyflakes:
        LOGGER.info("SKIPPING PYFLAKES TEST")
    else:
        LOGGER.info("testing code consistency with pyflakes...")
        cmd = (
            f"cd {TOP_DIR_PATH}; pyflakes .; cd {TEST_DIR_PATH}; pyflakes.; "
            f"cd {CWD}; exit 0"
        )
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True,
        )
        stdout, _ = proc.communicate()
        if stdout != "":
            LOGGER.error(
                f"ERROR: pyflakes check failed with output:\n{stdout}",
                exc_type=RuntimeError,
            )
        LOGGER.info("Passed pyflakes check : )")


def test_formatting(args):
    """
    If requested, run a formatting check with Black and check the output
    """
    if args.no_formatting:
        LOGGER.info("SKIPPING FORMATTING TEST")
    else:
        LOGGER.info("testing code formatting with Black...")
        cmd = (
            f"cd {TOP_DIR_PATH}; black openmsistream --check; black test --check; "
            f"cd {CWD}; exit 0"
        )
        with subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True,
        ) as proc:
            stdout, _ = proc.communicate()
            if stdout != "":
                LOGGER.error(
                    f"ERROR: formatting check failed with output:\n{stdout}",
                    exc_type=RuntimeError,
                )
            LOGGER.info("Passed formatting check : )")


def test_pylint(args):
    """
    If requested, run pylint and check the output for errors
    """
    if args.no_pylint:
        LOGGER.info("SKIPPING PYLINT TEST")
    else:
        LOGGER.info("testing code consistency with pylint...")
        cmd = (
            f"cd {TOP_DIR_PATH}; pylint openmsistream; pylint --recursive=y test; "
            f"cd {CWD}; exit 0"
        )
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=True,
            universal_newlines=True,
        )
        stdout, _ = proc.communicate()
        if stdout != "":
            LOGGER.error(
                f"ERROR: pylint checks failed with output:\n{stdout}",
                exc_type=RuntimeError,
            )
        LOGGER.info("Passed pylint checks : )")


def setup_local_broker():
    """
    Start up a local broker running in Docker and create the necessary topics in it
    """
    LOGGER.info("Setting up local Kafka broker")
    try:
        run_cmd_in_subprocess(
            ["bash", str(START_LOCAL_BROKER_SCRIPT_PATH)],
            logger=LOGGER,
            reraise=True,
            cwd=pathlib.Path(__file__).parent,
        )
    except Exception as exc:
        errmsg = (
            "ERROR: failed to set up the local testing broker: is Docker running? "
            "Exception will be re-raised."
        )
        LOGGER.error(errmsg, exc_info=exc, reraise=True)


def local_broker_is_running():
    """
    Returns True if the local Kafka broker is running, and False otherwise
    """
    try:
        client = docker.from_env()
        try:
            broker_name = "local_kafka_broker"
            broker_status = client.containers.get(broker_name).attrs["State"]["Status"]
            zk_name = "local_kafka_zookeeper"
            zookeeper_status = client.containers.get(zk_name).attrs["State"]["Status"]
            if broker_status == "running" and zookeeper_status == "running":
                return True
        except docker.errors.NotFound:
            return False
    except Exception as exc:
        LOGGER.error(
            "ERROR: failed to test whether the local broker is running! Will re-raise exception.",
            exc_info=exc,
            reraise=True,
        )
    return False


def start_local_broker_and_get_temp_env(args):
    """
    If requested, start up a local broker running in docker, create the necessary topics in it,
    and adjust the environment variables so that the tests will know to use it.
    Returns the local broker TemporaryEnvironment, or None if it's not needed
    """
    temp_env_local_broker = None
    if args.local_broker:
        LOGGER.info("Tests will be run using a local broker set up using Docker")
        if not local_broker_is_running():
            setup_local_broker()
        temp_env_var_dict = {
            "USE_LOCAL_KAFKA_BROKER_IN_TESTS": "yes",
            "LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS": "localhost:9092",
        }
        temp_env_local_broker = TemporaryEnvironment(temp_env_var_dict)
        temp_env_local_broker.__enter__()  # pylint: disable=unnecessary-dunder-call
    return temp_env_local_broker


def skip_kafka_tests_and_get_temp_env(args, suites):
    """
    If requested, un-set environment variables used for tests
    and set tests ending in "kafka" to skip

    Returns the TemporaryEnvironment to use for the "no_kafka" tests, or None if not necessary
    """
    temp_no_kafka_env = None
    if args.no_kafka:
        temp_env_var_dict = {"TESTS_NO_KAFKA": "yes"}
        for env_var_name in TEST_CONST.ENV_VAR_NAMES:
            temp_env_var_dict[env_var_name] = None
        temp_no_kafka_env = TemporaryEnvironment(temp_env_var_dict)
        temp_no_kafka_env.__enter__()  # pylint: disable=unnecessary-dunder-call
        for suite in suites:
            for test_group in suite._tests:  # pylint: disable=protected-access
                for test in test_group:
                    # pylint: disable=protected-access
                    if (test._testMethodName).endswith("kafka"):
                        # pylint: disable=protected-access
                        test_name = test._testMethodName
                        msg = (
                            "tests that interact with the kafka broker are being skipped"
                        )
                        setattr(
                            test, test_name, unittest.skip(msg)(getattr(test, test_name))
                        )
    return temp_no_kafka_env


def skip_unmatched_tests(args, suites):
    """
    If a regex was given to specify which tests to run, set any that don't match it to
    skip. Returns a TestSuite that contains only the suites to run
    """
    if args.test_regex is None:
        return suites
    filtered_suites = []
    if args.test_regex is not None:
        for suite in suites:
            some_test_will_run = False
            for test_group in suite._tests:  # pylint: disable=protected-access
                for test in test_group:
                    # pylint: disable=protected-access
                    if not args.test_regex.match(test._testMethodName):
                        # pylint: disable=protected-access
                        test_name = test._testMethodName
                        msg = (
                            f"tests that don't match the regex '{args.test_regex}' "
                            "are being skipped"
                        )
                        setattr(
                            test, test_name, unittest.skip(msg)(getattr(test, test_name))
                        )
                    else:
                        some_test_will_run = True
            if some_test_will_run:
                filtered_suites.append(suite)
    return unittest.TestSuite(filtered_suites)


def teardown_local_broker():
    """
    Stop the local Kafka broker
    """
    try:
        run_cmd_in_subprocess(
            ["bash", str(STOP_LOCAL_BROKER_SCRIPT_PATH)],
            logger=LOGGER,
            reraise=True,
            cwd=pathlib.Path(__file__).parent,
        )
        LOGGER.info("Local broker stopped")
    except Exception as exc:
        errmsg = (
            "ERROR: failed to stop the local testing broker, and it may need "
            "to be stopped manually. Exception will be re-raised."
        )
        LOGGER.error(errmsg, exc_info=exc, reraise=True)


def run_script_tests(args):
    """
    Run all requested tests defined in the "test_scripts" directory
    """
    if args.no_script_tests:
        LOGGER.info("SKIPPING SCRIPT TESTS")
    else:
        LOGGER.info(f"Running tests in {TEST_SCRIPT_DIR_PATH}...")
        # if we're going to use a local broker, start it, create the necessary topics in it,
        # and temporarily adjust the environment variables to run using it
        temp_env_local_broker = start_local_broker_and_get_temp_env(args)
        # load all of the tests into suites
        loader = unittest.TestLoader()
        suites = loader.discover(TEST_SCRIPT_DIR_PATH)
        if len(loader.errors) > 0:
            errmsg = "ERROR: encountered the following errors in loading tests:\n\t"
            errmsg += "\n\t".join([str(error) for error in loader.errors])
            LOGGER.error(errmsg, exc_type=RuntimeError)
        # if we're running in "no_kafka" mode, unset the environment variables
        # and set some tests to skip
        temp_no_kafka_env = skip_kafka_tests_and_get_temp_env(args, suites)
        # otherwise, if only some tests will be run. Set any that don't match the regex to skip
        suites = skip_unmatched_tests(args, suites)
        # actually run all of the requested tests
        runner_kwargs = {"verbosity": 3, "output": "test-results"}
        if args.failfast:
            runner_kwargs["failfast"] = True
        if not os.path.isdir("test-results"):
            os.mkdir("test-results")
        runner = xmlrunner.XMLTestRunner(**runner_kwargs)
        result = runner.run(suites)
        # exit the "no_kafka" TemporaryEnvironment
        if temp_no_kafka_env:
            temp_no_kafka_env.__exit__()
        # exit the "local broker" TemporaryEnvironment and stop the local broker
        if temp_env_local_broker:
            temp_env_local_broker.__exit__()
            if not args.skip_broker_teardown:
                teardown_local_broker()
        # check for any errors or failures
        if len(result.errors) > 0 or len(result.failures) > 0:
            raise RuntimeError(
                "ERROR: some test(s) failed! See output above for details."
            )
        LOGGER.info("All script tests complete : )")


def test_repo_is_clean(args):
    """
    If requested, make sure that the repository is still clean after running all the tests
    """
    if args.no_repo:
        LOGGER.info("SKIPPING GIT REPOSITORY CHECKS")
    else:
        LOGGER.info("SKIPPING GIT REPOSITORY CHECKS")
        # Commenting out repo checks for now (expect that KafkaCrypto files will update)
        ##make sure the Github repo is still clean from its initial state
        # LOGGER.info('Checking the status of the Git repo....')
        # p = subprocess.Popen(
        #   f'bash {TEST_REPO_STATUS_SCRIPT_PATH}',
        #   stdout=subprocess.PIPE,
        #   stderr=subprocess.PIPE,
        #   shell=True,
        #   cwd=TOP_DIR_PATH,
        #   #universal_newlines=True,
        # )
        # stdout,stderr = p.communicate()
        # try :
        #    stdout = stdout.decode()
        #    stderr = stderr.decode()
        # except :
        #    pass
        # if stdout!='' :
        #    LOGGER.error(
        #       f'ERROR: Git repo check failed with output:\n{stdout}',
        #       exc_type=RuntimeError,
        # )
        # LOGGER.info('Repo is good : )')


def main(args=None):
    """
    Main function to run the script
    """
    args = get_args(args)
    # If we're just setting up or tearing down the local broker, run just those functions
    if args.setup_local_broker:
        setup_local_broker()
        return
    if args.teardown_local_broker:
        teardown_local_broker()
        return
    test_pyflakes(args)
    test_formatting(args)
    test_pylint(args)
    run_script_tests(args)
    test_repo_is_clean(args)
    # If we've made it here all the (requested) tests passed!
    msg = "All "
    if (
        args.no_pyflakes
        or args.no_pylint
        or args.no_formatting
        or args.no_script_tests
        or args.no_kafka
        or args.test_regex
        or args.no_repo
    ):
        msg += "requested "
    msg += "tests passed!"
    LOGGER.info(msg)


if __name__ == "__main__":
    main()
