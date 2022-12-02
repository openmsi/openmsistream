================
Services/Daemons
================

**Any** of the :doc:`main programs <main_programs>` can be run from the command line or, alternatively, installed as Services on machines running Windows or daemons on machines running Linux. Services can be installed for all users of a particular machine and, once installed, they will run automatically from when the machine boots until it is stopped and/or removed. 

OpenMSIStream uses `the Non-Sucking Service Manager ("NSSM") <https://nssm.cc/>`_ to manage Windows Services. OpenMSIStream will try to download its own copy of the NSSM executable, but that functionality is rather unreliable, so users installing Windows Services are encouraged to download their own version of it `directly from the website here <https://nssm.cc/release/nssm-2.24.zip>`_, open the archive, and put the nssm.exe program in the directory in which they're running ``InstallService`` (see below).

On Linux systems, OpenMSIStream installs and interacts with daemons using "systemd". Most (but not all) Linux machines have systemd installed; OpenMSIStream will check for it and give you a command to install it (if you have such authority) if it's not found.

Programs running as Services or daemons cannot be interacted with from the command line after they've been installed, but they will still produce output to log files. The OpenMSIStream interface to install and interact with persistently-running instances of programs is the same regardless of running of Windows or Linux.

Preparing the environment (note for Windows)
--------------------------------------------

Issues with loading ``.dll`` files manifest differently when running OpenMSIStream code as Windows Services because Services run in %WinDir%/System32 and don't read the same PATH locations as running interactively. Some workarounds are built into OpenMSIStream to mitigate these problems, but if you run into trouble with missing ``.dll`` files they can typically be resolved by copying those files directly into the %WinDir%/System32 directory.

Setup and installation
----------------------

To install a Service or Daemon, type the following command in the `openmsi` environment in your Terminal or Anaconda Prompt (you will need to be in admin mode on Windows or have sudo privileges on Linux)::

    InstallService [program_name] [command_line_options] --service_name [name_for_service_or_daemon]

where:

* ``[program_name]`` is the name of a :doc:`main program <main_programs>`, 
* ``[command_line_options]`` is a placeholder for that main program's command line options, and 
* ``[name_for_service_or_daemon]`` is a unique name you'd like to use for the instance of the program (you'll need to remember this to keep working with the Service or daemon). 

Typing ``InstallService -h`` or ``InstallService [program_name] -h`` will give a helpful output detailing the required and optional command line arguments. These command line arguments will only need to be specified once, when the Service is installed.

Any optional command line arguments applicable to running a program are *also* applicable to installing the program as a Service or daemon. **PLEASE NOTE** that any arguments whose values are paths to directories or files **must be given as absolute paths** and not relative paths, because Services and daemons run from special locations independent of where you install them. This same principle holds for any parameter values in config files that are paths to files or directories. 

While the script runs, you may be prompted to input values for any environment variables referenced in config files. If you input any new environment variable values you will need to **close and reopen** the Terminal or Admin mode Anaconda Prompt you were working in and rerun the ``InstallService`` command (the script will exit itself, and you'll be prompted to do this).

If the script completes successfully on Windows, you should be able to see the Service listed in the Windows Service Manager window that pops up when you type ``mmc Services.msc``. The Service or daemon will not start running until it is "start"-ed using the command in the next section. After being started, a Linux daemon will be listed if you type ``top`` in the terminal.

Starting and interacting with Services and daemons
--------------------------------------------------

After installing a Service/daemon, you can use the ``ManageService [name_for_service_or_daemon] [run_mode]`` command to perform several actions on the program, where ``[run_mode]`` is a text command telling the ServiceManager what to do:

#. **start the program running** with ``[run_mode]`` given as "start". You **must** do this after installing the Service or daemon to get it running.
#. **check the program status** with ``[run_mode]`` given as "status" to make sure it's running properly
#. **stop the program running** with ``[run_mode]`` given as "stop". If you temporarily stop the Service using this command, you can restart it afterward with ``ManageService [name_for_service_or_daemon] start``.
#. **uninstall the Service or daemon completely** with ``[run_mode]`` given as "remove". You can also add the following flags when using this command:
    * ``--remove_env_vars`` to un-set any environment variable values
    * ``--remove_nssm`` to remove the NSSM executable that may have been downloaded to manage the Service on Windows 
    * ``--remove_install_args`` to remove the installation arguments file (which otherwise allows reinstalling the Service or daemon with the same arguments as before)
#. **reinstall the Service or daemon using arguments from the most recent install run** with ``[run_mode]`` given as "reinstall". This is a short-hand way to call the last-used version of ``InstallService`` for a service of this same name.
#. **do more than one of the above at once with compound run modes**. There are several compound run modes to do more than one action at once. They are :
    * "stop_and_remove" (calls "stop" and then "remove"), 
    * "stop_and_reinstall" (calls "stop", "remove", and then "reinstall"), and 
    * "stop_and_restart" (calls "stop", "remove", "reinstall", and then "start").

Debugging problems
------------------

If something goes wrong while the program is running as a Service/daemon, a file called ``[service_name]_ERROR_LOG.txt`` should be created in the directory that you ran the installation command from. That file should contain a traceback for the error that killed the Service. If that file doesn't exist, then there was likely an issue with installing the Service in the first place, and not with the OpenMSIStream Python code it was trying to run.

Output in the "working directory"
---------------------------------

Working with Services will create a few files in a "working directory". The default working directory is the `working_dir subdirectory of the OpenMSIStream repo <https://github.com/openmsi/openmsistream/tree/main/openmsistream/services/working_dir>`_, but you can change its location by setting the ``OPENMSISTREAM_SERVICES_WORKING_DIR`` environment variable on your system. A logfile called "Services.log" will contain some lines related to installing or working with any Services or daemons. Python files in that directory will correspond to the executables that are installed as Services, and so checking these files will give details on the setup for their corresponding Services/daemons. The directory may also contain text files listing environment variables values or installation arguments for services that you install. None of these created files will be tracked in the repo if they're in the default location (`they're in the .gitignore <https://github.com/openmsi/openmsistream/blob/main/.gitignore>`_).

Running other Python code as Services/Daemons
---------------------------------------------

In addition to the main programs provided by OpenMSIStream, the infrastructure used to work with Services/daemons can be applied to arbitrary Python code as well. To install your own code as a Service or daemon, you can run the same commands as above, except the ``InstallService`` command's ``[program_name]`` should be a specification string instead of the name of a main program. That specification string can have two different formats, depending on the code you want to install.

Installing any class that extends Runnable
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, if you've written your own new class that extends :class:`~.utilities.Runnable`, you can give the name of the class and the path to it as the specification string, like::

    InstallService [class_name]=[path.to.class.file] [command_line_options] --service_name [name_for_service_or_daemon]

where ``[class_name]`` is the name of the class, and ``[path.to.class.file]`` is a Python path to the file containing it. You can find an example custom Runnable class in the OpenMSIStream repository, called ``runnable_example.py`` in the ``openmsistream/services/examples`` directory. The :func:`openmsistream.utilities.Runnable.run_from_command_line` function in that class can be run as a Service by installing it with the command::

    InstallService RunnableExample=openmsistream.services.examples.runnable_example [absolute_path_to_output_dir] --service_name RunnableExampleServiceTest

and then starting/stopping/removing it with::

    ManageService RunnableExampleServiceTest start
    ManageService RunnableExampleServiceTest stop_and_remove

Running this successfully will create a file called ``runnable_example_service_test.txt`` in the directory at ``[absolute_path_to_output_dir]``; timestamped lines will be written to the file when the Python code is run. On Windows, the Service will be restarted each time it completes and so there will be several lines in the file and more will be added until the Service is stopped.

Installing a general Python function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If instead of a custom Runnable class you've written just a Python script containing a function you'd like to run, the specification string format is slightly different; the ``InstallService`` command would look like::

    InstallService [path.to.script.file]:[func_name] [command_line_options] --service_name [name_for_service_or_daemon]

where ``[path.to.script.file]`` is a Python path to the file containing the function to run, and ``[func_name]`` is the name of the function in the file. **The function you write should accept one argument:** the ``command_line_options`` as a list.

You can find an example custom Service/daemon script in the OpenMSIStream repository, called ``script_example.py`` in the ``openmsistream/services/examples`` directory. In this case the ``main`` function in the script can be run as a Service by installing it with the command::

    InstallService openmsistream.services.examples.script_example:main [absolute_path_to_output_dir] --service_name ScriptExampleServiceTest

and then starting/stopping/removing it with::

    ManageService ScriptExampleServiceTest start
    ManageService ScriptExampleServiceTest stop_and_remove

Running this successfully will create a file called ``script_example_service_test.txt`` in the directory at ``[absolute_path_to_output_dir]``; timestamped lines will be written to the file when the Python code is run. On Windows, the Service will be restarted each time it completes and so there will be several lines in the file and more will be added until the Service is stopped.
