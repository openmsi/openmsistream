Install libsodium
-----------------

libsodium is a package used for the `KafkaCrypto <https://github.com/tmcqueen-materials/kafkacrypto>`_ package that provides the end-to-end data encryption capability of OpenMSIStream. Since encryption is a built-in option for OpenMSIStream, you must install libsodium even if you don't want to use encryption. Install the libsodium package through Miniconda using the shell command::

    conda install -c anaconda libsodium
