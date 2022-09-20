---
title: 'OpenMSIStream: A Python package facilitating integration of streaming data into diverse laboratory environments'
tags:
  - Python
  - data streaming
  - science data
  - Apache Kafka
  - materials science
authors:
  - name: Margaret Eminizer
    corresponding: true
    orcid: 0000-0003-4591-2225
    affiliation: 1
  - name: Sam Tabrisky
    affiliation: "2, 3, 4"
  - name: Amir Sharifzadeh
    affiliation: 1
  - name: Christopher DiMarco
    affiliation: 4
  - name: Jacob Diamond
    orcid: 0000-0001-7905-4260
    affiliation: 4
  - name: Kaliat Ramesh
    orcid: 0000-0003-2659-4698
    affiliation: 4
  - name: Todd Hufnagel
    orcid: 0000-0002-6373-9377
    affiliation: "4, 5, 6"
  - name: Tyrel McQueen
    orcid: 0000-0002-8493-4630
    affiliation: "5, 7, 8"
  - name: David Elbert
    orcid: 0000-0002-2292-180X
    affiliation: "4, 9"
affiliations:
 - name: Institute for Data Intensive Engineering and Science (IDIES), The Johns Hopkins University, USA
   index: 1
 - name: Department of Biology, Dartmouth College, USA
   index: 2
 - name: Department of Computer Science, Dartmouth College, USA
   index: 3
 - name: Hopkins Extreme Materials Institute (HEMI), The Johns Hopkins University, USA
   index: 4
 - name: Department of Materials Science and Engineering, The Johns Hopkins University, USA
   index: 5
 - name: Department of Mechanical Engineering, The Johns Hopkins University, USA
   index: 6
 - name: Department of Chemistry, The Johns Hopkins University, USA
   index: 7
 - name: Institute for Quantum Matter (IQM), The Johns Hopkins University, USA
   index: 8
 - name: Department of Earth and Planetary Sciences, The Johns Hopkins University, USA
   index: 9
date: 20 September 2022
bibliography: paper.bib
---

# Summary

"Streaming data" is a general term to describe data that are continuously generated from any number of sources, often in relatively small packets or "records". Streaming data are typically stored centrally for some period of time and then processed sequentially either individually or in small groups. The resulting flows of raw data, metadata, and processing results form "ecosystems" that automate arbitrary data-driven tasks. Data streaming is an extension of the publish-subscribe ("pub/sub") messaging pattern, under which senders (or publishers) of data are completely independent of the recipients (or subscribers) of those data. Message-focused middleware solutions such as RabbitMQ [citation], Apache Pulsar [citation], and Apache Kafka [@kafka:2022] all provide unique approaches to this pattern.

The availability of streaming data has revolutionized many industries in the past decade, creating entirely new standards of practice and types of available analytics. The general scientific community has also benefitted from incorporating data streams into workflows at different scales. Streaming data can be used in scientific applications to solve such disparate problems as centralizing data backups, performing data analytics, instrument monitoring, and metadata handling, and managing workflows either within or between different participants in projects, all automated in real time as data flow continuously from sources.

# Statement of Need

OpenMSIStream simplifies the process of standing up streaming data ecosystems by abstracting some details while still providing full functionality and configurability to interested users. OpenMSIStream provides programs to easily break single "data files" with arbitrary contents on disk into smaller "chunks" that form the fundamental records in independently-configured streams. Those records can be read back to reconstruct the original data file contents, triggering arbitrary code to run as entire files become available from the stream in real time. 

The messaging backend for OpenMSIStream is provided by the $\texttt{confluent_kafka}$ Python wrapper [citation] around Apache Kafka. A Kafka "broker" persistently stores messages in ordered, append-only logs called "topics". "Producer" programs send messages to be appended to topics, while "consumer" programs read messages stored in those topics. 

On the producer side, OpenMSIStream provides programs to upload single files to Kafka topics, and persistently watch directory trees on file systems for files to upload. On the consumer side, OpenMSIStream programs can download files uploaded to topics to disk, or transfer files to S3 [citation] object store buckets as they become available. OpenMSIStream also includes base classes that users can easily extend to invoke arbitrary Python code on the contents of reconstructed files and save processing results locally or produce them as messages to different topics, including a specific implementation for automated extraction and re-production of metadata keys and values.

OpenMSIStream programs (or extensions thereof) can be run from the command line, in Docker containers, or installed to run persistently in the background as Windows Services or Linux daemons, all using the same simple interface. Producer and consumer programs and the central Kafka broker exist independently of one another, so they can run on computers where data are being generated by instruments, on machines hosting data storage, or on more powerful servers for analysis as necessary. The Kafka backend is fully customizable through a simple config file interface, and data uploaded to topics can optionally be encrypted on the broker using KafkaCrypto [citation].

OpenMSIStream was designed for deployment in diverse science laboratory environments. Lab scientist or student users need only minimal computing experience to set up a directory on an instrument computer to watch for data files and start running another program on a different computer to automate backups or transfers to local disks or cloud storage solutions. Slightly more advanced users can adapt their existing analysis codes in Python (or other programming languages!) to automatically perform analyses in real time and save results locally or send them off to another Kafka topic.

In materials science projects, which formed the inspiration for OpenMSIStream, it is common to see iterative scientific design workflows synthesizing contributions from several different labs that focus on material production, simulation, and characterization. Using data streaming to pass raw data, metadata, and analysis or simulation results automatically between these groups increases interoperability to tighten the materials design loop. 

OpenMSIStream is currently being used to automate data transfer between laboratories at Cornell University and storage facilities at Johns Hopkins University as part of the PARADIM distributed materials innovation platform [citation], and for similar purposes at the Materials Characterization and Processing (MCP) Facility at Johns Hopkins University [citation]. OpenMSIStream will be deployed in the near future as part of the data streaming solutions for the High-Throughput Materials Discovery for Extreme Conditions (HTMDEC) project at the Army Research Laboratory [citation], as well as in [DAVID PLEASE ADVISE: VICKI'S DMREF MAAP PROJECT AT UMASS LOWELL]. OpenMSIStream will be adapted as the streaming solution for the Open Material Semantic Infrastructure (OpenMSI) Designing Materials to Revolutionize and Engineer our Future (DMREF) collaboration at the Hopkins Extreme Materials Institute (HEMI).

Because it is written in Python, OpenMSIStream interfaces seamlessly with other existing components of scientific software stacks such as 
numpy [citation], SciPy [citation], and pandas [citation], as well as the BlueSky data collection framework [citation] which also uses Apache Kafka for its streaming backend. The use of the Kafka backend also allows users even more familiar with the Kafka ecosystem to take full advantage of non-Python tools like Kafka Streams [citation] for further data handling outside of OpenMSIStream.

# Acknowledgments

The development of OpenMSIStream has been financially supported by NSF Awards #1921959, #1539918, and #2129051.
