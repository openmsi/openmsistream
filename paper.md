---
title: 'OpenMSIStream: A Python package for facilitating integration of streaming data in diverse laboratory environments'
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
    orcid: 0000-0002-4100-4898
    affiliation: 1
  - name: Christopher DiMarco
    orcid: 0000-0002-2267-938X
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

OpenMSIStream provides seamless connection of scientific data stores with streaming infrastructure to allow researchers to leverage the power of decoupled, real-time data streaming architectures. Data streaming is the process of transmitting, ingesting, and processing data continuously rather than in batches. Access to streaming data has revolutionized many industries in the past decade and created entirely new standards of practice and types of analytics. While rare in science research settings, data streaming has proven a key technology to reap the potential of rapid advances in scientific data collection [@bluesky].  This paucity of streaming infrastructures linking complex scientific systems is due to a lack of tools that facilitate streaming in the diverse and distributed systems common in modern research.  OpenMSIStream closes the gap between underlying streaming systems and common scientific infrastructure. Closing this gap empowers novel streaming applications for scientific data including automation of data curation, reduction, and analysis; real-time experiment monitoring and control; and flexible deployment of AI/ML to guide autonomous research.

Streaming data generally refers to data continuously generated from multiple sources and passed in small packets termed messages. Streaming data messages are typically organized in groups called topics and persisted for periods of time conducive to processing for multiple uses either sequentially or in small groups. The resulting flows of raw data, metadata, and processing results form "ecosystems" that automate varied data-driven tasks. A strength of data streaming ecosystems is the use of publish-subscribe ("pub/sub") messaging backbones, that decouple data senders (publishers) and recipients (subscribers). Popular message-focused middleware solutions such as RabbitMQ [@rabbitMQ], Apache Pulsar [@pulsar], and Apache Kafka [@kafka] all provide differing capabilities as backbones. OpenMSIStream provides robust and efficient, yet easy, access to the rich data streaming systems of Apache Kafka.

# Statement of Need

The majority of scientific research today relies on semi-automated collection, reduction, and analysis of data. Vast improvements in instrumentation and computational modeling, however, have rapidly increased the volume and quality of that data, providing an opportunity for transformative acceleration of science.  Such transformation will require scalable integration of data resources for development and real-time deployment of AI and machine learning to facilitate pervasive laboratory automation and the development of autonomous decision making in research.

OpenMSIStream development was driven by needs in Materials Science research with its central goal of discovering novel materials to meet urgent societal needs in fields as diverse as energy, health, the environment, privacy, and security. OpenMSIStream provides a tool to accelerate materials research through the development and integration of data and data platform resources in the Materials Innovative Infrastructures prioritized the Materials Genome Initiative (MGI) (e.g. TMS study 2017, strategic plan).  Specifically, OpenMSIStream simplifies the process of standing up streaming systems by abstracting details while still providing full functionality and configurability. OpenMSIStream provides file-oriented tools to align with the prevailing paradigms of scientific instrumentation and data analysis. The OpenMSIStream suite of tools manage chunking of data files of any type to form manageable messages in independently-configured topics. Records can be read back to reconstruct the original data file contents and trigger flexible processing code to run as entire files become available from the stream in real time. 

The messaging backend for OpenMSIStream is provided by the $\texttt{confluent\_kafka}$ Python wrapper [@confluentkafka] around Apache Kafka. A Kafka "broker" persistently stores messages in ordered, append-only logs called "topics". "Producer" programs send messages to be appended to topics, while "consumer" programs read messages stored in those topics. 

OpenMSIStream producers provide flexibility to upload single files to Kafka topics, and persistently watch directory trees on file systems for files to upload. OpenMSIStream consumers can download files uploaded to topics to disk, or transfer files to object stores through S3 API compliance [@awssss]. OpenMSIStream also includes base classes that users can extend to invoke individualized Python code on the contents of reconstructed files and save processing results locally or produce them as messages to different topics, including a specific implementation for automated extraction and re-production of metadata keys and values.

OpenMSIStream programs (or extensions thereof) can be run from the command line, in Docker containers, or installed to run persistently in the background as Windows Services or Linux daemons, all using the same simple interface. Producer and consumer programs and the central Kafka broker exist independently of one another, so they can run on computers where data are being generated by instruments, on machines hosting data storage, or on more powerful servers for analysis as necessary. The Kafka backend is fully customizable through a simple config file interface, and data uploaded to topics can optionally be encrypted on the broker using KafkaCrypto [@kafkacrypto].

OpenMSIStream was designed for deployment in diverse science laboratory environments. Lab scientist or student users need only minimal computing experience to set up a directory on an instrument computer to watch for data files and start running another program on a different computer to automate backups or transfers to local disks or cloud storage solutions. Slightly more advanced users can adapt their existing analysis codes to automatically perform analyses in real time and save results locally or send them off to another Kafka topic.

In materials science projects, it is common to see iterative scientific design workflows integrating contributions from several different labs that focus on material production, simulation, and characterization. Using data streaming to pass raw data, metadata, and analysis or simulation results automatically between these groups increases interoperability to tighten the materials design loop. 

OpenMSIStream is currently being used to automate data transfer and analysis between electron microscopy laboratories at Cornell University and Johns Hopkins University as part of the PARADIM distributed Materials Innovation Platform [citation], and for similar purposes for X-ray instruments at the Materials Characterization and Processing (MCP) Facility at Johns Hopkins University [citation]. OpenMSIStream will be deployed in the near future as part of the data streaming solutions for the High-Throughput Materials Discovery for Extreme Conditions (HTMDEC) project [citation], as well as in [DAVID PLEASE ADVISE: VICKI'S DMREF MAAP PROJECT AT UMASS LOWELL]. OpenMSIStream will be adapted as the streaming solution for the Open Material Semantic Infrastructure (OpenMSI) Designing Materials to Revolutionize and Engineer our Future (DMREF) collaboration at the Hopkins Extreme Materials Institute (HEMI).

Because it is written in Python, OpenMSIStream interfaces seamlessly with other existing components of scientific software stacks such as 
$\texttt{NumPy}$ [@numpy], $\texttt{SciPy}$ [@scipy], and $\texttt{pandas}$ [@pandas], as well as the Bluesky data collection framework [@bluesky] which also uses Apache Kafka for its streaming backend. The use of the Kafka backend also allows users even more familiar with the Kafka ecosystem to take full advantage of non-Python tools like Kafka Streams [@kafkastreams] for further data handling outside of OpenMSIStream.

# Acknowledgments

The development of OpenMSIStream has been financially supported by NSF Awards #1921959, #1539918, and #2129051.

# References
