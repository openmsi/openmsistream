## Automatic code tests

There are several tests for the codebase already written (and more will be added over time). The repo also has a continuous integration workflow set up on CircleCI, and the checks in that workflow must pass successfully on any branch being merged into main via a Pull Request.  

### Running tests interactively 

If you're editing the code, you can make sure it doesn't break anything currently being tested by running `python test/run_all_tests.py` (the file [here](./run_all_tests.py)) from just inside the directory of the repo. If you'd like to add more tests, you can include any classes that extend `unittest.TestCase` in the [`test/unittests` subdirectory](./unittests). If you name their files anything that starts with `test`, the `run_all_tests.py` script will run them automatically. If any new test methods interact with the Kafka broker, you should end their names with "`kafka`" so that `run_all_tests.py` can exclude them if requested. Running the tests also requires that `pyflakes` and a few other dependencies be installed, which you can get right from this repo by running `pip install .[test]` (with or without the `--editable` or `-e` flag(s)).

There are also a few options you can add to `run_all_tests.py` if you only want to run some subset of the available tests:
1. Add the "`--no_pyflakes`" flag to skip the pyflakes test
1. Add the "`--no_unittests`" flag to skip the unittests entirely, OR
1. Add the "`--no_kafka`" flag to skip running tests that need to communicate with the Kafka broker. Adding this flag automatically **skips any test methods whose names end with "`kafka`"**; you will see that they were skipped at the end of the output.
1. Add the "`--unittest_regex [regex]`" option to skip any tests whose function names aren't matched to the `[regex]` regular expression
1. Add the "`--no_repo`" flag to skip the test of whether the Git repo is still clean after running all tests
1. Add the "`--failfast`" flag to stop executing the script early if any individual test(s) fail. Normally all tests are run regardless of how many fail, but including this flag will stop the run as soon as any test fails.

### Continuous Integration with CircleCI

Continuous integration for the repo is set up using CircleCI. On their website you can manually run tests on any branch you'd like, and the tests will also automatically be run when pull requests are submitted. The configuration for CircleCI is in the `.circleci/config.yml` [file](../.circleci/config.yml) in the root directory of the repository. Running tests successfully on CircleCI requires that the Project on CircleCI has environment variables for the test broker username and password registered within it, as well as environment variables to allow access to the IDIES/SciServer Docker registry.

### Rebuilding static test data

Some of the tests rely on static example data in `test/data`. If you need to regenerate these data under some new conditions (i.e., because you've changed default options someplace), you can run `python test/rebuild_test_reference_data.py` and follow the prompts it gives you to replace the necessary files. You can also add to that script if you write any new tests that rely on example data. Static test data should be committed to the repo like any other file, and they'll be picked up both interactively and on CircleCI.
