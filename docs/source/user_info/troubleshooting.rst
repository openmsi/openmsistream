===============
Troubleshooting
===============

This page describes some potential solutions to problems that users have run into when installing or working with OpenMSIStream. 

Missing .dll files on Windows
-----------------------------

Due to the wide variety of Windows builds, setting the conda environment variable to modify the DLL search path may not solve all issues stemming from the "librdkafka.dll" file seemingly missing. `See here for more context on this problem <https://github.com/confluentinc/confluent-kafka-python/issues/1221>`_. OpenMSIStream will make its own best efforts to find and pre-load the librdkafka.dll file on Windows, but it may not always be possible to find it. The general fix for this issue is to find the librdkafka file and copy it to a location referenced by your system PATH, unless it's showing up when running programs as Services, in which case :ref:`you should copy it to %WinDir%/System32 <Preparing the environment (quick note for Windows)>`. 

Another common issue with Windows builds is a seemingly missing "libsodium.dll" file. If you encounter errors stating trouble importing ``pysodium``, make sure the directory containing your "libsodium.dll" is added to your system PATH, or copied into %WinDir%/System32 as applicable.

Mac OS "active file descriptors" error
--------------------------------------

Some Mac machines may run into an esoteric issue related to the number of active file descriptors, which appears as repeated error messages like::

    % ERROR: Local: Host resolution failure: kafka-xyz.example.com:9092/42: Failed to resolve 'kafka-xyz.example.com:9092': nodename nor servname provided, or not known (after 0ms in state CONNECT)

when the Kafka server is otherwise available and should be fine, especially when using relatively large numbers of parallel threads. Instead of the above error, you may instead get "too many open files" errors.

These errors may be due to running out of file descriptors as discussed in `this known confluent-kafka/librdkafka issue <https://github.com/edenhill/kcat/issues/209>`_. Using a broker hosted on Confluent Cloud may increase the likelihood of getting errors like these, because librdkafka creates two separate file descriptors for each known broker regardless of whether a connection is established. 

If you type ``ulimit -n`` into a Terminal window and get an output like ``256``, it's likely this is the cause. To solve this issue, you will need to increase the limit of the number of allowed file descriptors, by running ``ulimit -n 4096``. If that makes the errors go away, then you might want to add that line to your shell ``.profile`` or ``.rc`` file.

Older operating systems and SSL errors
--------------------------------------

Some machines may experience errors in connecting to Kafka brokers because their operating systems are old enough to have a set of ca certificates that won't work with new certificates on many sites. If you see repeated errors like::

    FAIL|rdkafka#consumer-2| [thrd:ssl://kafka-xyz.example.com:9092/42]: ssl://kafka-xyz.example.com:9092/42: SSL handshake failed: error:1416F086:SSL routines:tls_process_server_certificate:certificate verify failed: broker certificate could not be verified, verify that ssl.ca.location is correctly configured or root CA certificates are installed (install ca-certificates package)

you should be able to solve this issue by installing "certifi" with pip::

    pip install --upgrade certifi

and then adding the location of the CA file it installed to the ``[broker]`` section of your config file as ``ssl.ca.location``. You can find the location of the CA file by running a couple lines in Python:

    >>> import certifi
    >>> certifi.where()
