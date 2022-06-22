=============================
Getting Started
=============================

We recommend using a minimal installation of the conda open source package and environment management system. These instructions start with installation of conda and outline all the necessary steps to run OpenMSIStream programs. Working with OpenMSIStream requires sending data through *topics* served by a *broker*.  In practice that means you will need access to a Kafka broker running on a server or in the cloud, and you will need to create topics on the broker to hold the data streams.  If these concepts are new to you we suggest contacting us for assistance and/or using a simple, managed cloud solution, such as `Confluent Cloud <https://confluent.cloud/>`_, as your broker. 

Quick start with miniconda3 
---------------------------

We recommend using miniconda3 for the lightest installation. miniconda3 installers can be downloaded from `the website here <https://docs.conda.io/en/latest/miniconda.html>`_, and installation instructions can be found `here <https://conda.io/projects/conda/en/latest/user-guide/install/index.html>`_.

Finishing installation
----------------------

The pages below list specific installation instructions based on the operating system you're running:

.. toctree::
   :maxdepth: 1

   installation/linux
   installation/older_windows
   installation/windows
   installation/mac_intel
   installation/mac_m1
