=============================
Introduction to OpenMSIStream
=============================

OpenMSIStream is a collection of Python programs and modules that use the `confluent_kafka <https://github.com/confluentinc/confluent-kafka-python>`_ wrapper around `Apache Kafka <https://kafka.apache.org/>`_ to simplify adding arbitrary file data to Kafka topics, downloading the files in those topics to different endpoints, and processing the files (or portions of them) as they move through topics (i.e. for realtime data analysis or metadata extraction). Because Open MSI has a particular focus on materials science data, OpenMSIStream has been written specifically to transfer materials data between different lab machines or S3 buckets and facilitate processing data in flight to increase interoperability between portions of projects and tighten the materials design loop.

The main programs offered by OpenMSIStream include streamlined ways to:

* Upload single files to Kafka topics
* Persistently watch directory trees on file systems and upload any files added to them to Kafka topics
* Download files uploaded to Kafka topics to disk
* Download files uploaded to Kafka topics to S3 object store buckets
* Call arbitrary Python code to process the contents of files uploaded to Kafka topics as they are made available

Producer-type (upload) and Consumer-type (download/process) programs and the central Kafka broker exist completely independently of one another, providing flexibility for a variety of data ecosystems. The Kafka backend used is fully customizable through a simple config file interface, and data uploaded to topics can optionally be encrypted using `KafkaCrypto <https://github.com/tmcqueen-materials/kafkacrypto>`_. 

OpenMSIStream is designed to run on Windows or Linux machines, though it supports Mac OS as well. Data producers typically run on Windows or Linux computers that run collection of data on laboratory instruments; data consumers and stream processors run on the same computers, on servers with more compute power as needed, or where storage of data is hosted.  In all these cases, programs are run in Python 3 in virtual environments or Docker containers, used interactively from the command line or run as always-available Services (on Windows) or daemons (on Linux). 

