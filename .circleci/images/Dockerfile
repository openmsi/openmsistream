# CircleCI image from https://hub.docker.com/r/cimg/python
FROM cimg/python:3.9

# Switch to root user
USER root

#start in the default area
WORKDIR /

#install libsodium
RUN apt-get update -y && \
    apt-get install -y libsodium-dev

#clone the openmsistream repo and install it
RUN pip install openmsistream[all]
