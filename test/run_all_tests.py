#imports
import unittest, subprocess, pathlib, re, time
from argparse import ArgumentParser
from tempenv import TemporaryEnvironment
from openmsistream.utilities import Logger
from openmsistream.services.utilities import run_cmd_in_subprocess
from test_scripts.placeholder_env_vars import ENV_VAR_NAMES

#constants
TOP_DIR_PATH = (pathlib.Path(__file__).parent.parent).resolve()
START_LOCAL_BROKER_SCRIPT_PATH = (pathlib.Path(__file__).parent / 'start_local_broker.sh').resolve()
CREATE_LOCAL_TESTING_TOPICS_SCRIPT_PATH = (pathlib.Path(__file__).parent / 'create_local_testing_topics.sh').resolve()
STOP_LOCAL_BROKER_SCRIPT_PATH = (pathlib.Path(__file__).parent / 'stop_local_broker.sh').resolve()
TEST_SCRIPT_DIR_PATH = (pathlib.Path(__file__).parent / 'test_scripts').resolve()
TEST_REPO_STATUS_SCRIPT_PATH = (pathlib.Path(__file__).parent / 'test_repo_status.sh').resolve()
CWD = pathlib.Path().resolve()
LOGGER = Logger('run_all_tests')

def get_args(args) :
    """
    Return the parsed command line arguments
    """
    parser = ArgumentParser()
    parser.add_argument('--no_pyflakes', action='store_true',
                        help='Add this flag to skip running the pyflakes check')
    parser.add_argument('--no_pylint', action='store_true',
                        help='Add this flag to skip running the pylint checks')
    script_test_opts = parser.add_mutually_exclusive_group()
    script_test_opts.add_argument('--no_script_tests', action='store_true',
                                  help=f'Add this flag to skip running the tests in {TEST_SCRIPT_DIR_PATH.name}')
    script_test_opts.add_argument('--no_kafka', action='store_true',
                                  help=f'Add this flag to skip running the tests in {TEST_SCRIPT_DIR_PATH.name}')
    script_test_opts.add_argument('--test_regex',type=re.compile,default=None,
                                  help='Only tests whose function names match this regex will be run')
    parser.add_argument('--local_broker',action='store_true',
                        help='''Add this flag to automatically set up a local Kafka broker with Docker 
                                and use that broker to run tests instead of a third-party broker 
                                configured using environment variables''')
    parser.add_argument('--no_repo', action='store_true',
                        help='Add this flag to skip running the Git repository checks')
    parser.add_argument('--failfast', action='store_true',
                        help='Add this flag to exit after the first failed test')
    return parser.parse_args(args=args)

def test_pyflakes(args) :
    """
    If requested, run pyflakes and check the output for errors
    """
    if args.no_pyflakes :
        LOGGER.info('SKIPPING PYFLAKES TEST')
    else :
        LOGGER.info('testing code consistency with pyflakes...')
        p = subprocess.Popen(f'cd {TOP_DIR_PATH}; pyflakes .; cd {CWD}; exit 0',
                             stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True,universal_newlines=True)
        stdout,_ = p.communicate()
        if stdout!='' :
            LOGGER.error(f'ERROR: pyflakes check failed with output:\n{stdout}',exc_type=RuntimeError)
        LOGGER.info('Passed pyflakes check : )')

def test_pylint(args) :
    """
    If requested, run pylint and check the output for errors
    """
    if args.no_pylint :
        LOGGER.info('SKIPPING PYLINT TEST')
    else :
        LOGGER.info('testing code consistency with pylint...')
        p = subprocess.Popen(f'cd {TOP_DIR_PATH}; pylint openmsistream; cd {CWD}; exit 0',
                             stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True,universal_newlines=True)
        stdout,_ = p.communicate()
        if stdout!='' :
            LOGGER.error(f'ERROR: pylint checks failed with output:\n{stdout}',exc_type=RuntimeError)
        LOGGER.info('Passed pylint checks : )')

def start_local_broker_and_get_temp_env(args) :
    """
    If requested, start up a local broker running in docker, create the necessary topics in it,
    and adjust the environment variables so that the tests will know to use it.
    Returns the local broker TemporaryEnvironment, or None if it's not needed
    """
    temp_env_local_broker = None
    if args.local_broker :
        LOGGER.info('Tests will be run using a local broker set up using Docker instead of a third-party broker')
        try :
            run_cmd_in_subprocess(
                ['sh',str(START_LOCAL_BROKER_SCRIPT_PATH)],
                logger=LOGGER,
                reraise=True,
                cwd=pathlib.Path(__file__).parent,
            )
            LOGGER.info('Local broker started, will sleep for 5 seconds to give it a moment to get up and running')
            time.sleep(5)
            run_cmd_in_subprocess(
                ['sh',str(CREATE_LOCAL_TESTING_TOPICS_SCRIPT_PATH)],
                logger=LOGGER,
                reraise=True,
                cwd=pathlib.Path(__file__).parent,
            )
            LOGGER.info('Testing topics created in local broker')
        except Exception as exc :
            errmsg = 'ERROR: failed to set up the local testing broker and topics. Exception will be re-raised.'
            LOGGER.error(errmsg,exc_info=exc,reraise=True)
        temp_env_var_dict = {
            'USE_LOCAL_KAFKA_BROKER_IN_TESTS':'yes',
            'LOCAL_KAFKA_BROKER_BOOTSTRAP_SERVERS':'localhost:9092',
            }
        temp_env_local_broker = TemporaryEnvironment(temp_env_var_dict)
        temp_env_local_broker.__enter__()
    return temp_env_local_broker

def skip_kafka_tests_and_get_temp_env(args,suites) :
    """
    If requested, un-set environment variables used for tests and set tests ending in "kafka" to skip

    Returns the TemporaryEnvironment to use for the "no_kafka" tests, or None if not necessary
    """
    temp_no_kafka_env = None
    if args.no_kafka :
        temp_env_var_dict = {}
        for env_var_name in ENV_VAR_NAMES :
            temp_env_var_dict[env_var_name]=None
        temp_no_kafka_env = TemporaryEnvironment(temp_env_var_dict)
        temp_no_kafka_env.__enter__()
        for suite in suites :
            for test_group in suite._tests :
                for test in test_group :
                    if (test._testMethodName).endswith('kafka') :
                        test_name = test._testMethodName
                        msg = 'tests that interact with the kafka broker are being skipped'
                        setattr(test, test_name, 
                                unittest.skip(msg)(getattr(test, test_name)))
    return temp_no_kafka_env

def skip_unmatched_tests(args,suites) :
    """
    If a regex was given to specify which tests to run, set any that don't match it to skip
    """
    if args.test_regex is not None :
        for suite in suites :
            for test_group in suite._tests :
                for test in test_group :
                    if not args.test_regex.match(test._testMethodName) :
                        test_name = test._testMethodName
                        msg = f"tests that don't match the regex '{args.test_regex}' are being skipped"
                        setattr(test, test_name, 
                                unittest.skip(msg)(getattr(test, test_name)))

def run_script_tests(args) :
    """
    Run all requested tests defined in the "test_scripts" directory
    """
    if args.no_script_tests :
        LOGGER.info('SKIPPING SCRIPT TESTS')
    else :
        LOGGER.info(f'Running tests in {TEST_SCRIPT_DIR_PATH}...')
        # if we're going to use a local broker, start it, create the necessary topics in it,
        # and temporarily adjust the environment variables to run using it
        temp_env_local_broker = start_local_broker_and_get_temp_env(args)
        # load all of the tests into suites
        loader = unittest.TestLoader()
        suites = loader.discover(TEST_SCRIPT_DIR_PATH)
        if len(loader.errors)>0 :
            errmsg = 'ERROR: encountered the following errors in loading tests:\n\t'
            errmsg += '\n\t'.join([str(error) for error in loader.errors])
            LOGGER.error(errmsg,exc_type=RuntimeError)
        # if we're running in "no_kafka" mode, unset the environment variables and set some tests to skip
        temp_no_kafka_env = skip_kafka_tests_and_get_temp_env(args,suites)
        # otherwise, if only some tests will be run. Set any that don't match the regex to skip
        skip_unmatched_tests(args,suites)
        #actually run all of the requested tests
        runner_kwargs = {'verbosity':3}
        if args.failfast :
            runner_kwargs['failfast'] = True
        runner = unittest.TextTestRunner(**runner_kwargs)
        result = runner.run(suites)
        #exit the "no_kafka" TemporaryEnvironment
        if temp_no_kafka_env :
            temp_no_kafka_env.__exit__()
        #exit the "local broker" TemporaryEnvironment and stop the local broker
        if temp_env_local_broker :
            temp_env_local_broker.__exit__()
            try :
                run_cmd_in_subprocess(
                    ['sh',str(STOP_LOCAL_BROKER_SCRIPT_PATH)],
                    logger=LOGGER,
                    reraise=True,
                    cwd=pathlib.Path(__file__).parent,
                )
                LOGGER.info('Local broker stopped')
            except Exception as exc :
                errmsg = 'ERROR: failed to stop the local testing broker, and it may need to be stopped manually. '
                errmsg+= 'Exception will be re-raised.'
                LOGGER.error(errmsg,exc_info=exc,reraise=True)
        #check for any errors or failures
        if len(result.errors)>0 or len(result.failures)>0 :
            raise RuntimeError('ERROR: some test(s) failed! See output above for details.')
        LOGGER.info('All script tests complete : )')

def test_repo_is_clean(args) :
    """
    If requested, make sure that the repository is still clean after running all the tests
    """
    if args.no_repo :
        LOGGER.info('SKIPPING GIT REPOSITORY CHECKS')
    else :
        LOGGER.info('SKIPPING GIT REPOSITORY CHECKS')
        # Commenting out repo checks for now (expect that KafkaCrypto files will update)
        ##make sure the Github repo is still clean from its initial state
        #LOGGER.info('Checking the status of the Git repo....')
        #p = subprocess.Popen(f'sh {TEST_REPO_STATUS_SCRIPT_PATH}',stdout=subprocess.PIPE,stderr=subprocess.PIPE,
        #                     shell=True,cwd=TOP_DIR_PATH)#,universal_newlines=True)
        #stdout,stderr = p.communicate()
        #try :
        #    stdout = stdout.decode()
        #    stderr = stderr.decode()
        #except :
        #    pass
        #if stdout!='' :
        #    LOGGER.error(f'ERROR: Git repo check failed with output:\n{stdout}',exc_type=RuntimeError)
        #LOGGER.info('Repo is good : )')

def main(args=None) :
    args = get_args(args)
    test_pyflakes(args)
    test_pylint(args)
    run_script_tests(args)
    test_repo_is_clean(args)
    # If we've made it here all the (requested) tests passed!
    msg = 'All '
    if args.no_pyflakes or args.no_pylint or args.no_script_tests or args.no_kafka or args.test_regex or args.no_repo :
        msg+='requested '
    msg+='tests passed!'
    LOGGER.info(msg)

if __name__=='__main__' :
    main()
