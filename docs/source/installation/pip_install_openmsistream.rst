Install OpenMSIStream
---------------------

You can install the OpenMSIStream code through PyPI::

    pip install openmsistream

If you'd like to be able to make changes to the OpenMSIStream code without reinstalling, you can include the ``--editable`` flag in the ``pip install`` command. If you'd like to run the automatic code tests or build the documentation on your local system, you can install the optional dependencies needed with ``pip install .[test]`` or ``pip install .[docs]``, respectively, with or without the ``--editable`` flag.

**This completes installation and will give you access to several new console commands to run OpenMSIStream applications, as well as any of the other modules in the openmsistream package.**

If you like, you can check your installation by opening a python prompt and typing:

    >>> import openmsistream

and if that line runs without any problems then the package was installed correctly.
