================================
Mac OS (Intel chip) installation
================================

OpenMSIStream is not officially supported on Mac OS, but its interactive programs work reliably at this time.

.. include:: create_conda_39.rst

.. include:: libsodium.rst

Install librdkafka
------------------

Working with MacOS requires a *system-wide* install of librdkafka. The easiest way to install librdkafka on Macs is using the package manager homebrew. You can find the (brief) instructions for installing homebrew `here <https://brew.sh/>`_. 

You may also need to install Xcode command line tools before librdkafka. After installing homebrew, you can install the command line tools and librdkafka with::

    xcode-select --install
    brew install librdkafka

.. include:: pip_install_openmsistream.rst
