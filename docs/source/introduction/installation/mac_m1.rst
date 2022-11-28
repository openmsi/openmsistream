===================================
Mac OS (Apple M1 chip) installation
===================================

OpenMSIStream is not designed to run in Mac OS environments, but its programs work reliably on Mac at this time.

.. include:: create_conda_39.rst

.. include:: libsodium.rst

Install librdkafka
------------------

Working with MacOS requires a *system-wide* install of librdkafka. The easiest way to install librdkafka on Macs is using the package manager homebrew. The librdkafka installation instructions below were tested on a system running OS X Monterey; your mileage may vary.

Change the default shell to Bash::
 
    chsh -s /bin/bash

Install Homebrew::
 
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

Add homebrew bin and sbin locations to your path::
 
    export PATH=/opt/homebrew/bin:$PATH
    export PATH=/opt/homebrew/sbin:$PATH

Use brew to install librdkafka::
 
    brew install librdkafka

.. include:: pip_install_openmsistream.rst
