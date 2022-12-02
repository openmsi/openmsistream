=============================
Introduction to OpenMSIStream
=============================

OpenMSIStream is a collection of Python programs and modules that facilitate the incorporation of data streaming into scientific data handling and analysis workflows.

Streaming data in general
-------------------------

"Streaming data" is a very general term to describe data that are continuously generated from any number of sources, often in relatively small packets or "records". Records are typically stored centrally for some period of time, with the intention to process them sequentially and individually or in relatively small groups. The resulting flows of raw data, metadata, and processing results form "ecosystems" of automated data-driven tasks, which stay up to date as individual records are made available from their sources in real time.

Data streaming is an extension of the publish-subscribe ("pub/sub") messaging pattern, under which senders (or publishers) of data are completely independent of the recipients (or subscribers) of those data. Message-focused middleware solutions such as `RabbitMQ <https://www.rabbitmq.com/>`_, `Apache Pulsar <https://pulsar.apache.org/>`_, and `Apache Kafka <https://kafka.apache.org/>`_ all provide their own approaches to this pattern, suitable for different use cases.

The availability of streaming data has revolutionized a wide variety of industries in the past decade, creating entirely new standards of practice and types of available analytics. The general scientific community has also benefitted from incorporating data streams into workflows on different scales. Streaming data can be used in scientific applications to solve such disparate problems as centralizing data backups, performing data analytics and instrument monitoring, handling metadata, and managing workflows either within or between different participants in projects, all automated in real time.

What OpenMSIStream provides
---------------------------

OpenMSIStream simplifies the process of standing up streaming data ecosystems by abstracting some details of how its particular backend solution works, while still providing its full functionality and configurability to interested users. OpenMSIStream provides programs to easily break single "data files" with arbitrary contents on disk into smaller "chunks" that form the fundamental records in streams. Those records can be read back by other OpenMSIStream programs to reconstruct the original data file contents, triggering arbitrary code to run as entire files become available from the stream in real time. 

The messaging backend for OpenMSIStream is provided by Confluent, Inc.'s `confluent_kafka <https://github.com/confluentinc/confluent-kafka-python>`_ Python wrapper around `Apache Kafka <https://kafka.apache.org/>`_. Kafka-based architectures in general have three primary types of components: brokers, producers, and consumers. A Kafka broker persistently stores messages in ordered, append-only logs called "topics" and manages receiving and sending messages to and from those topics. Producer programs send messages to be appended to topics, while consumer programs read messages stored in those topics. 

The main programs offered by OpenMSIStream include streamlined ways to:

* :doc:`Upload single files to Kafka topics <../user_info/main_programs/upload_data_file>`
* :doc:`Persistently watch directory trees on file systems and upload any files added to them to Kafka topics <../user_info/main_programs/data_file_upload_directory>`
* :doc:`Download files uploaded to Kafka topics to disk <../user_info/main_programs/data_file_download_directory>`
* :doc:`Download files uploaded to Kafka topics and transfer them to S3 object store buckets <../user_info/main_programs/s3_transfer_stream_processor>`

OpenMSIStream also includes some base classes that can be extended by users to:

* :doc:`Call arbitrary Python code to process files uploaded to Kafka topics as they are made available <../user_info/base_classes/data_file_stream_processor>` (to keep local logs of files in flight, for example, or to save analysis results locally, potentially to be re-produced to other topics)
* :doc:`Call arbitrary Python code to analyze data files consumed from one Kafka topic and produce the analysis results to another Kafka topic <../user_info/base_classes/data_file_stream_reproducer>`
* :doc:`Extract metadata from data files consumed from one Kafka topic and produce it to another Kafka topic <../user_info/base_classes/metadata_json_reproducer>`

OpenMSIStream programs (or custom extensions of the provided base classes) can be run interactively from the command line, in Docker containers, or installed to run persistently in the background as Windows Services or Linux daemons, all using the same simple command line-like interface. Producer-type (upload) and consumer-type (download/process) programs and the central Kafka broker exist independently of one another, so they can run on computers where data are being generated by instruments, on machines hosting data storage, or on more powerful servers for analysis as necessary. The Kafka backend is fully customizable through a simple config file interface, and data uploaded to topics can optionally be encrypted on the broker using `KafkaCrypto <https://github.com/tmcqueen-materials/kafkacrypto>`_. 

OpenMSIStream userbase
----------------------

OpenMSIStream has been designed with a specific focus on deployment in diverse science laboratory environments. It is easy to install in lightweight virtual environments on Windows and Linux machines, and runs reliably on Mac OS as well. Lab scientist or student users need only minimal computing experience to set up a directory to watch for data files on an instrument computer, and another program on a different computer to, for example, automatically back up every file added to that directory to a configured S3 cloud storage solution. Slightly more advanced users can adapt their existing analysis code in Python (or other programming languages!) to automatically perform analyses in real time and save results locally or send them off to another Kafka topic.

The particular use cases motivating the development of OpenMSIStream are materials science projects, where it is common to see iterative scientific design workflows synthesizing contributions from several different labs. For example, one group may create a material according to some design while another group characterizes that material using any number of experimental techniques, and a third group uses the results of those characterization experiments to run simulations that inform a next generation of material design. Using data streaming to pass raw data, metadata, and analysis or simulation results automatically between these groups increases interoperability to tighten this design loop.

Related work
------------

Because it is written in Python, OpenMSIStream interfaces seamlessly with other existing components of scientific software stacks such as `numpy <https://numpy.org/>`_, `SciPy <https://scipy.org/>`_, and `pandas <https://pandas.pydata.org/>`_. The `BlueSky data collection framework <https://nsls-ii.github.io/bluesky/index.html>`_ developed at Brookhaven National Laboratory is another example of open source Python scientific software focused on laboratory environments and analysis using Apache Kafka for its streaming backend. The use of the Kafka backend also allows users more familiar with the Kafka ecosystem to take full advantage of non-Python tools like `Kafka Streams <https://kafka.apache.org/documentation/streams/>`_ for further procesing of data outside of the lightweight OpenMSIStream ecosystem.

Next steps
----------

Users can proceed next to the :doc:`installation instructions <installing_openmsistream>` to start working with OpenMSIStream. The :doc:`tutorial section <tutorials>` walks through some simple, local examples using test data and is a great place to continue on from there.

The :doc:`page on the main OpenMSIStream programs <../user_info/main_programs>` provides links to the documents describing the functionality of each program with specific instructions for how to run them, as well as descriptions of the extensible base classes and instructions for how to write adaptations of them. 

The :doc:`page here <../user_info/services>` describes how to easily install programs as Windows Services or Linux daemons, and the :doc:`page here <../user_info/encryption>` describes how to encrypt messages stored on the broker using KafkaCrypto. Some solutions to common troubleshooting issues are discussed on the :doc:`page here <../user_info/troubleshooting>`.

Users seeking support, wishing to report issues, or wanting to contribute to the OpenMSIStream project can find details on how to do so on the :doc:`page here <../user_info/support_and_contribution>`, and an API reference for the project is provided :doc:`here <../dev_info/api_reference>`.

Acknowledgments
---------------

Financial support for the development of OpenMSIStream has been provided under NSF awards 1921959 (DMREF: Data-Driven Integration of Experiments and Multi-Scale Modeling for Accelerated Development of Aluminum Alloys), 1539918 (MIP: Platform for the Accelerated Realization, Analysis, and Discovery of Interface Materials (PARADIM)), and 2129051 (Data CI Pilot: VariMat Streaming Polystore Integration of Varied Experimental Materials Data).
