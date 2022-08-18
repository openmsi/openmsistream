This directory contains two different examples of files that could be used to install custom Python code as Windows Services or Linux daemons. Both files just write out a small text file to a location specified by a command line argument. 

The `runnable_example.py` file contains the definition of a class that extends [openmsistream.running.Runnable](../../running/runnable.py). In this case the `run_from_command_line` function in the class can be run as a Service by installing it with the command:

```
InstallService RunnableExample=openmsistream.services.examples.runnable_example [absolute_path_to_output_dir] --service_name RunnableExampleServiceTest
```

and then starting/stopping/removing it with:

```
ManageService RunnableExampleServiceTest start
ManageService RunnableExampleServiceTest stop_and_remove
```

Running this successfully will create a file called `runnable_example_service_test_[timestamp].txt` in the directory at `[absolute_path_to_output_dir]`; timestamped lines will continue to be written to the file as the Service is automatically rerun until is is stopped.

The `script_example.py` file contains an example of a simple script to install as a Service. In this case the `main` function in the script can be run as a Service by installing it with the command:

```
InstallService openmsistream.services.examples.script_example:main [absolute_path_to_output_dir] --service_name ScriptExampleServiceTest
```

and then starting/stopping/removing it with:

```
ManageService RunnableExampleServiceTest start
ManageService RunnableExampleServiceTest stop_and_remove
```

Running this successfully will create a file called `script_example_service_test_[timestamp].txt` in the directory at `[absolute_path_to_output_dir]`; timestamped lines will continue to be written to the file as the Service is automatically rerun until is is stopped.