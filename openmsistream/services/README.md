## Installing programs as Windows Services and Linux Daemons

**Any** of the available top-level programs can be run from the command line or, alternatively, installed as Services on machines running Windows or Daemons on machines running Linux (with `systemd` installed). Services can be installed for all users of a particular machine and, once installed, they will run automatically from when the machine boots until it is stopped and/or removed. Programs running as Windows Services cannot be interacted with from the command line after they've been installed, but they will still produce output to log files.

### Preparing the environment (note for Windows)

Issues with loading `.dll` files manifest differently when running `OpenMSIStream` code as Windows Services because Services run in `%WinDir%\System32` and don't read the same `PATH` locations as running interactively. Some workarounds are built into `OpenMSIStream` to mitigate these problems, but if you run into trouble with missing `.dll` files they can typically be resolved by copying those files into the `%WinDir%\System32` directory.

### Setup and installation

To install a Service or Daemon, type the following command in the `openmsi` environment in your Terminal or Anaconda Prompt (you will need to be in admin mode):

`InstallService [program_name] [command_line_options] --service_name [name_for_service_or_daemon]`

where `[program_name]` is the name of a top-level program (corresponding to a Python Class), `[command_line_options]` is a placeholder for that top-level program's command line options, and `[name_for_service_or_daemon]` is a unique name you'd like to use for the instance of the program. Typing `InstallService -h` or `InstallService [program_name] -h` will give a helpful output detailing the required and optional command line arguments. These command line arguments will only need to be specified once, when the Service is installed.

Any optional command line arguments applicable to running a program are also applicable to installing the program as a Service. **PLEASE NOTE** that any arguments whose values are paths to directories or files **must be given as absolute paths** and not relative paths, otherwise the underlying code may crash if it can't find a specified path, and even debugging this error using the executable may be opaque. 

While the script runs, you may be prompted to input values for any needed environment variables. If you input any new environment variable values you will need to close and reopen the Terminal or Admin mode Anaconda Prompt you were working in and rerun the `InstallService` command.

If the script completes successfully on Windows, you should be able to see the Service listed in the Windows Service Manager window that pops up when you type `mmc Services.msc`. The Service will not start running until it is "start"-ed using the command in the next section.

### Starting Services and other interaction

After installing a Service/Daemon, you can use the `ManageService [name_for_service_or_daemon] [run_mode]` command to perform several actions on the program running as a Service where `[run_mode]` is a text command telling the ServiceManager what to do:
1. **start the Service running** with `[run_mode]` given as "`start`". You must do this after installing the Service to get it running.
1. **check the Service status** with `[run_mode]` given as "`status`" to make sure it's running properly
1. **stop the Service running** with `[run_mode]` given as "`stop`". If you temporarily stop the Service using this command, you can restart it afterward with `ManageService [name_for_service_or_daemon] start`.
1. **uninstall the Service completely** with `[run_mode]` given as "`remove`". If it was the only Service running on the machine, you can also add the `--remove_env_vars` flag to un-set the username/password environment variables. You can also remove the NSSM executable by adding the `--remove_nssm` flag, and/or remove the installation arguments file (which allows reinstalling the Service) by adding the `--remove_install_args` flag.
1. **reinstall the Service using arguments from the most recent install run** with `[run_mode]` given as "`reinstall`". This is a short-hand way to call the last-used version of `InstallService` for a service of this same name.
1. **do more than one of the above at once with compound run modes**. There are several compound run modes to do more than one action at once. They are `stop_and_remove` (calls `stop` and then `remove`), `stop_and_reinstall` (calls `stop`, `remove`, and then `reinstall`), and `stop_and_restart` (calls `stop`, `remove`, `reinstall`, and then `start`).

### Debugging problems

If something goes wrong while the program is running as a Service/daemon, a file called `[service_name]_ERROR_LOG.txt` should be created in the directory that you ran the installation command from. That file should contain a traceback for the error that killed the Service. If that file doesn't exist, then there was likely an issue with installing the Service in the first place, and not with the `OpenMSIStream` code that was run.

### Output in the "working directory"

Working with Services will create a few files in the ["`working_dir`" subdirectory](./working_dir). A logfile called "Services.log" will contain some lines related to installing or working with any Services. Python files in that directory will correspond to the executables that are installed as Services, and so checking these files will give details on the setup for their corresponding Services. None of these created files will be tracked in the repo (they're in the [gitignore](../../.gitignore)).
