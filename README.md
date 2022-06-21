# <div align="center"> Open MSI Stream </div>
#### <div align="center">***v0.9.1.3***</div>

#### <div align="center">Maggie Eminizer<sup>2</sup>, Amir Sharifzadeh<sup>2</sup>, Sam Tabrisky<sup>3</sup>, Alakarthika Ulaganathan<sup>4</sup>, David Elbert<sup>1</sup></div>

 <div align="center"><sup>1</sup>Hopkins Extreme Materials Institute (HEMI), PARADIM Materials Innovation Platform, and Dept. of Earth and Planetary Sciences, The Johns Hopkins University, Baltimore, MD, USA</div>
  <div align="center"><sup>2</sup>Institute for Data Intensive Engineering and Science (IDIES), Dept. of Physics and Astronomy, The Johns Hopkins University, Baltimore, MD, USA</div>
 <div align="center"><sup>3</sup>Depts. of Biology and Computer Science, Dartmouth College, Hanover, NH, and HEMI, The Johns Hopkins University, Baltimore, MD, USA</div> 
 <div align="center"><sup>4</sup>Dept. of Applied Mathematics and Statistics, The Johns Hopkins University, Baltimore, MD, USA</div>
 <br>

![PyPI](https://img.shields.io/pypi/v/openmsistream) ![GitHub](https://img.shields.io/github/license/openmsi/openmsistream) ![PyPI - Python Version](https://img.shields.io/pypi/pyversions/openmsistream) ![GitHub Release Date](https://img.shields.io/github/release-date/openmsi/openmsistream) ![GitHub last commit](https://img.shields.io/github/last-commit/openmsi/openmsistream) ![CircleCI](https://img.shields.io/circleci/build/github/openmsi/openmsistream/main)

## Introduction

Applications for laboratory, analysis, and computational materials data streaming using [Apache Kafka](https://kafka.apache.org/)

Available on GitHub at https://github.com/openmsi/openmsistream

Developed for Open MSI (NSF DMREF award #1921959)

Programs use the Python implementation of the Apache Kafka API, and are designed to run on Windows, Mac or Linux machines.  Data producers typically run on Windows or Linux computers that run collection of data on laboratory instruments; data consumers and stream processors run on the same computers, on servers with more compute power as needed, or where storage of data is hosted.  In all these cases, Open MSI components are run in Python 3 in virtual environments or Docker containers. Open MSI can be used interactively from the command line or run as an always-available service (on Windows) or daemon (on Linux).  

## Installation

We recommend using a minimal installation of the conda open source package management system and environment management system. These installation instructions start with installation of conda and outline all the necessary steps to run Open MSI tools.  To run Open MSI usefully, you need to understand that data streams through *topics* served by a *broker*.  In practice that means you will need access to a broker running on a server or in the cloud somewhere and you will need to create topics on the broker to hold the streams.  If these concepts are new to you we suggest contacting us for assistance and/or using a simple, managed cloud solution, such as Confluent Cloud, as your broker. 

### Quick Start 

#### Overview of Installation:

Here is an outline of the installation steps that are detailed below: 

1. Install miniconda3 (if not already installed)
2. Create and use a conda virtual environment dedicated to openmsistream
3. Install libsodium in that dedicated environment
4. (Optional: Install librdkafka manually if using a Mac)
5. Install OpenMSIStream in the dedicated environment
6. Write environment variables (usually the best choice, but optional)
7. Provision the KafkaCrypto node that should be used (if encryption is required)
8. Write a config file to use
9. Install and start the Service (Windows) or Daemon (Linux) (Usual for production use, but optional when experimenting with OpenMSIStream)

**NOTE:** Please read the entire installation section below before proceeding.  There are specific differences between instructions for Windows, Linux, Intel-MacOS, and M1-MacOS. 

#### 1. miniconda3 Installation

We recommend using miniconda3 for the lightest installation. miniconda3 installers can be downloaded from [the website here](https://docs.conda.io/en/latest/miniconda.html), and installation instructions can be found on [the website here](https://conda.io/projects/conda/en/latest/user-guide/install/index.html).

#### 2. Conda Environment Creation

With Miniconda installed, create and activate a dedicated virtual environment for OpenMSI. In a terminal shell (or Anaconda Prompt in admin mode on Windows) type:

```
conda create -n openmsi python=3.9
conda activate openmsi
```
##### N.B. Different Conda Step for Older Versions of Windows:

Python 3.9 is not supported on Windows 7 or earlier. Installations on pre-Windows 10 systems should, therefore, use Python 3.7 instead of Python 3.9, in which case, replace the two commands above with:

```
conda create -n openmsi python=3.7
conda activate openmsi
```

*In principle* `OpenMSIStream` code is transparent to the difference between Python 3.7 and 3.9, but it is recommended to use newer Windows systems that can support Python 3.9

###### Issues with DLL Files:

On Windows, you need to set a special variable in the virtual environment to allow the Kafka Python code to find its dependencies (see [here](https://github.com/ContinuumIO/anaconda-issues/issues/12475) for more details). To do this, activate your Conda environment as above then type the following commands to set the variable and then refresh the environment:

```
conda env config vars set CONDA_DLL_SEARCH_MODIFICATION_ENABLE=1
conda deactivate 
conda activate openmsi
```

###### More Details about Versions:

At the time of writing, Python 3.9 is the most recent release of Python supported by confluent-kafka on Windows 10, and is recommended for most deployments. 

##### General Virtual Environment Note:

No matter the operating system, you'll need to use the second command to "activate" the openmsi environment every time you open a Terminal window or Anaconda Prompt and want to work with OpenMSIStream. 

#### 3. Install libsodium

`libsodium` is a package used for the [KafkaCrypto](https://github.com/tmcqueen-materials/kafkacrypto) package that provides the end-to-end data encryption capability of `OpenMSIStream`. Since encryption is a built-in option for `OpenMSIStream`, you must install `libsodium` even if you don't want to use encryption. Install the `libsodium` package through Miniconda using the shell command:

`conda install -c anaconda libsodium`

#### 4. Install librdkafka for Macs (this step not required on Windows)

MacOS is not officially supported for `OpenMSIStream`, but works reliably at this time. If you would like to work with MacOS you will, however, need to install `librdkafka` using the package manager homebrew. The process is different on Intel-Chip Macs than on newer Apple-Silicon, M1 Mac.  

##### On Intel MacOS:

This may also require installing Xcode command line tools. You can install both of these using the commands below:

```
xcode-select --install
brew install librdkafka
```

##### On M1 MacOS (tested on Monterey system):

1. Change the default shell to Bash:
 
    `chsh -s /bin/bash`

2. Install Homebrew:
 
    `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`

3. Add homebrew bin and sbin locations to your path:
 
    `export PATH=/opt/homebrew/bin:$PATH`
    `export PATH=/opt/homebrew/sbin:$PATH`

4. Use brew to install *librdkafka*:
 
    `brew install librdkafka`

#### 5. Install OpenMSIStream

`pip install openmsistream`

If you'd like to be able to make changes to the `openmsistream` code without reinstalling, you can include the `--editable` flag in the `pip install` command. If you'd like to run the automatic code tests, you can install the optional dependencies needed with `pip install .[all]` with or without the `--editable` flag.

#### Installation Completion Note:

**This completes installation and will give you access to several new console commands to run `OpenMSIStream` applications, as well as any of the other modules in the `openmsistream` package.**

If you like, you can check your installation with:

```
python

>>> import openmsistream
```

and if that line runs without any problems then the package was installed correctly.

#### 6. Environment variables (for Confluent Cloud or S3 Object Stores)

Interacting with the Kafka broker on Confluent Cloud, including running code tests as described [here](./test), requires that some environment variables are set on your system. If you're installing any software to run as a Windows Service or Linux Daemon (as described [here](./openmsistream/services)) then you'll be prompted to enter these variables' values, but you may find it more convenient to set them once and save them as environment variables. 

The environment variables are called `KAFKA_TEST_CLUSTER_USERNAME`, `KAFKA_TEST_CLUSTER_PASSWORD`, `KAFKA_PROD_CLUSTER_USERNAME`, and `KAFKA_PROD_CLUSTER_PASSWORD`. The "`TEST`" variables are used to connect to the Kafka broker cluster for testing and must be set for developers needing to successfully run the automatic code tests. The "`PROD`" variables are used to connect to a full production cluster and are only needed for fully deployed code.

You can set these environment variables in a shell `.rc` or `.profile` file if running on Linux or Mac OS. On Windows you can set them as machine environment variables using commands like:

```
[Environment]::SetEnvironmentVariable("NAME","VALUE",[EnvironmentVariableTarget]::Machine)
```

You can also set them as "User" or "Process" environment variables on Windows if you don't have the necessary permissions to set them for the "Machine". 

Secret keys for connecting to S3 object stores should similarly be stored as environment variables and referenced in config files, rather than hard-coded. For running tests, the S3 object store environment variable names are `ACCESS_SECRET_KEY_ID`, `BUCKET_NAME`, `ENDPOINT_URL`, `REGION`, and `SECRET_KEY_ID`.

#### 7. Provision KafkaCrypto Node (optional)

The [readme file here](./openmsistream/my_kafka) explains how to enable message-layer encryption through [KafkaCrypto](https://github.com/tmcqueen-materials/kafkacrypto).

#### 8. Write Config File

The [readme file here](./openmsistream/my_kafka) also explains options for configuration files used to define which kafka cluster(s) the programs interact with and how data are produced to/consumed from topics within them.

#### 9. Install and Start Service or Daemon (optional)

The [readme file here](./openmsistream/services) explains options and installation of OpenMSIStream programs continually as Windows Services or Linux Daemons.  

## Other documentation

Installing the code provides access to several programs that share a basic scheme for user interaction. These programs share the following attributes:
1. Their names correspond to names of Python Classes within the code base
1. They can be run from the command line by typing their names 
    * i.e. they are provided as "console script entry points"
    * check the relevant section of the [setup.py](./setup.py) file for a list of all that are available
1. They can be installed as Windows Services instead of run from the bare command line

The documentation for specific programs can be found in a few locations within the repo. 

The [readme file here](./openmsistream/data_file_io/) explains programs used to upload entire arbitrary files by breaking them into chunks/producing those chunks as messages to a Kafka topic or download entire files by reading messages from the topic and writing data to disk.

The [readme file here](./openmsistream/my_kafka) explains how to enable message-layer encryption through [KafkaCrypto](https://github.com/tmcqueen-materials/kafkacrypto), and gives more details about options for configuration files used to define which kafka cluster(s) the programs interact with and how data are produced to/consumed from topics within them.

The [readme file here](./openmsistream/osn) explains how to consume files to arbitrary S3 buckets instead of saving them locally.

The [readme file here](./openmsistream/services) details procedures for installing any available command-line program as a Windows Service or Linux Daemon and working with it.

The [readme file here](./test) describes the automatic testing and CI/CD setup for the project, including how to run tests interactively and add additional tests.

## Troubleshooting

#### Missing .dll files on Windows

Due to the wide variety of Windows builds, setting the conda environment variable above may not solve all issues stemming from the `librdkafka.dll` file seemingly missing. See [here](https://github.com/confluentinc/confluent-kafka-python/issues/1221) for more context on this problem. A fix for the most common cases is built into `OpenMSIStream` and can be found [here](./openmsistream/__init__.py); referencing that code may be helpful in resolving any remaining `librdkafka`/`confluent-kafka-python` issues.

Another common issue with Windows builds is a seemingly missing `libsodium.dll` file. If you encounter errors stating trouble importing `pysodium`, make sure the directory containing your `libsodium.dll` is added to your `PATH`, or that the `libsodium.dll` file is properly registered on your Windows system.

##### Missing .dll files running programs as Windows Services

Issues with loading `.dll` files manifest differently when running `OpenMSIStream` code as Windows Services because Services run in `%WinDir%\System32` and don't read the same `PATH` locations as running interactively. Some workarounds are built into `OpenMSIStream` to mitigate these problems, but if you run into trouble with missing `.dll` files while running `OpenMSIStream` programs as Services they can typically be resolved by copying those files into the `%WinDir%\System32` directory.

#### Mac OS "active file descriptors" error

Some Mac machines may run into an esoteric issue related to the number of active file descriptors, which appears as repeated error messages like

`% ERROR: Local: Host resolution failure: kafka-xyz.example.com:9092/42: Failed to resolve 'kafka-xyz.example.com:9092': nodename nor servname provided, or not known (after 0ms in state CONNECT)`

when the Kafka server is otherwise available and should be fine, especially when using relatively large numbers of parallel threads. Instead of the above error, you may get `too many open files` errors.

These errors may be due to running out of file descriptors as discussed in [this known `confluent-kafka`/`librdkafka` issue](https://github.com/edenhill/kcat/issues/209): using a broker hosted on Confluent Cloud may increase the likelihood of getting errors like these, because `librdkafka` creates two separate file descriptors for each known broker regardless of whether a connection is established. If you type `ulimit -n` into a Terminal window and get an output like `256`, it's likely this is the cause. To solve this issue, you will need to increase the limit of the number of allowed file descriptors, by running `ulimit -n 4096`. If that makes the errors go away, then you might want to add that line to your shell `.profile` or `.rc` file.

#### Older operating systems and SSL errors

Some machines may experience errors in connecting to Kafka brokers because their operating systems are old enough to have a set of ca certificates that won't work with new certificates on many sites. If you see repeated errors like

`FAIL|rdkafka#consumer-2| [thrd:ssl://kafka-xyz.example.com:9092/42]: ssl://kafka-xyz.example.com:9092/42: SSL handshake failed: error:1416F086:SSL routines:tls_process_server_certificate:certificate verify failed: broker certificate could not be verified, verify that ssl.ca.location is correctly configured or root CA certificates are installed (install ca-certificates package)`

you should be able to solve this issue by installing "certifi" with pip:

```
pip install --upgrade certifi
```

and then adding the location of the CA file it installed to the `[broker]` section of your config file as `ssl.ca.location`. You can find the location of the CA file by running a couple lines in Python:

```
>>> import certifi
>>> certifi.where()
```

## To-do list

The following items are currently planned to be implemented ASAP:

1. New applications for asynchronous and repeatable stream filtering and processing (i.e. to facilitate decentralized/asynchronous lab data analysis)
1. Allowing watching directories where large files are in the process of being created/saved instead of just directories where fully-created files are being added
1. Implementing other data types and serialization schemas, likely using Avro
1. Create pypi and conda installations. Pypi method using twine here: https://github.com/bast/pypi-howto. Putting on conda-forge is a heavier lift. Need to decide if it's worth it; probably not for such an immature package.
1. Re-implement PDV plots from a submodule

## Questions that will arise later (in FAQs?)

1. What happens if we send very large files to topics to be consumed to an object store? (Might not be able to transfer GB of data at once?)
1. How robust is the object store we're using (automatic backups, etc.)
