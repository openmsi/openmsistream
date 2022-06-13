#imports
import unittest, subprocess, pathlib, re
from argparse import ArgumentParser

#constants
TOP_DIR_PATH = (pathlib.Path(__file__).parent.parent).resolve()
UNITTEST_DIR_PATH = (pathlib.Path(__file__).parent / 'unittests').resolve()
TEST_REPO_STATUS_SCRIPT_PATH = (pathlib.Path(__file__).parent / 'test_repo_status.sh').resolve()
CWD = pathlib.Path().resolve()

# TODO: osn test case

def main(args=None) :
    #make the argument parser
    parser = ArgumentParser()
    parser.add_argument('--no_pyflakes', action='store_true',
                        help='Add this flag to skip running the pyflakes check')
    unittest_opts = parser.add_mutually_exclusive_group()
    unittest_opts.add_argument('--no_unittests', action='store_true',
                               help='Add this flag to skip running the unittest checks')
    unittest_opts.add_argument('--no_kafka', action='store_true',
                               help='Add this flag to skip running the unittest checks')
    unittest_opts.add_argument('--unittest_regex',type=re.compile,default=None,
                               help='Only unittests whose function names match this regex will be run')
    parser.add_argument('--no_repo', action='store_true',
                        help='Add this flag to skip running the Git repository checks')
    parser.add_argument('--failfast', action='store_true',
                        help='Add this flag to exit after the first failed test')
    args = parser.parse_args(args=args)
    #test pyflakes
    if args.no_pyflakes :
        print('SKIPPING PYFLAKES TEST')
    else :
        print('testing code consistency with pyflakes...')
        p = subprocess.Popen(f'cd {TOP_DIR_PATH}; pyflakes .; cd {CWD}; exit 0',
                             stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=True,universal_newlines=True)
        stdout,stderr = p.communicate()
        if stdout!='' :
            raise RuntimeError(f'ERROR: pyflakes check failed with output:\n{stdout}')
        print('Passed pyflakes check : )')
    #perform all the unittests
    if args.no_unittests :
        print('SKIPPING UNITTESTS')
    else :
        print(f'Running unittests in {UNITTEST_DIR_PATH}...')
        loader = unittest.TestLoader()
        suites = loader.discover(UNITTEST_DIR_PATH)
        if len(loader.errors)>0 :
            print('ERROR: encountered the following errors in loading tests:')
            for error in loader.errors :
                print(f'\t{error}')
            return
        if args.no_kafka :
            for suite in suites :
                for test_group in suite._tests :
                    for test in test_group :
                        if (test._testMethodName).endswith('kafka') :
                            test_name = test._testMethodName
                            msg = 'tests that interact with the kafka broker are being skipped'
                            setattr(test, test_name, 
                                    unittest.skip(msg)(getattr(test, test_name)))
        elif args.unittest_regex is not None :
            for suite in suites :
                for test_group in suite._tests :
                    for test in test_group :
                        if not args.unittest_regex.match(test._testMethodName) :
                            test_name = test._testMethodName
                            msg = f"tests that don't match the regex '{args.unittest_regex}' are being skipped"
                            setattr(test, test_name, 
                                    unittest.skip(msg)(getattr(test, test_name)))
        runner_kwargs = {'verbosity':3}
        if args.failfast :
            runner_kwargs['failfast'] = True
        runner = unittest.TextTestRunner(**runner_kwargs)
        result = runner.run(suites)
        if len(result.errors)>0 or len(result.failures)>0 :
            raise RuntimeError('ERROR: some unittest(s) failed! See output above for details.')
            return
        print('All unittest checks complete : )')
    if args.no_repo :
        print('SKIPPING GIT REPOSITORY CHECKS')
    else :
        print('SKIPPING GIT REPOSITORY CHECKS')
        # Commenting out repo checks for now (expect that KafkaCrypto files will update)
        ##make sure the Github repo is still clean from its initial state
        #print('Checking the status of the Git repo....')
        #p = subprocess.Popen(f'sh {TEST_REPO_STATUS_SCRIPT_PATH}',stdout=subprocess.PIPE,stderr=subprocess.PIPE,
        #                     shell=True,cwd=TOP_DIR_PATH)#,universal_newlines=True)
        #stdout,stderr = p.communicate()
        #try :
        #    stdout = stdout.decode()
        #    stderr = stderr.decode()
        #except :
        #    pass
        #if stdout!='' :
        #    raise RuntimeError(f'ERROR: Git repo check failed with output:\n{stdout}')
        #print('Repo is good : )')
    #If we've made it here all the (requested) tests passed!
    msg = 'All '
    if args.no_pyflakes or args.no_unittests or args.no_repo :
        msg+='requested '
    msg+='tests passed!'
    print(msg)

if __name__=='__main__' :
    main()
