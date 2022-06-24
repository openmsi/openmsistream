=============================
Windows 7 installation
=============================

Python 3.9 is not supported on Windows 7 or earlier. Installations on those older systems should, therefore, use Python 3.7 instead of Python 3.9. *In principle* OpenMSIStream code is transparent to the difference between Python 3.7 and 3.9, but it is recommended to use newer Windows systems that can support Python 3.9. Since that's not always an option on lab instruments, though, we provide installation instructions for those older Windows systems here.

.. include:: create_conda_37.rst

.. include:: dll_files.rst

.. include:: libsodium.rst

.. include:: pip_install_openmsistream.rst
