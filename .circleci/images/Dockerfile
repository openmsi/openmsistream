# CircleCI image from https://hub.docker.com/r/cimg/python
FROM cimg/python:3.9

# Switch to root user
USER root

#start in the default area
WORKDIR /

#install libsodium
RUN apt-get update -y && \
    apt-get install -y libsodium-dev

#install git
RUN apt-get install -y git

#clone the openmsistream repo and install it
RUN git clone https://github.com/openmsi/openmsistream.git
WORKDIR /openmsistream
RUN pip install .[all]

#switch back to the default area
WORKDIR /
