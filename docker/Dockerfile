# base image is official "slim" Python 3.9 on Debian 11 ("bullseye")
FROM python:3.9-slim-bullseye

# Install libsodium
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends libsodium-dev && \
    apt-get autoremove -y

# Install OpenMSIStream
RUN pip install --upgrade pip && \
    pip install openmsistream && \
    pip cache purge

# Use /home as the working directory
WORKDIR /home

# Use a bash Entrypoint
ENTRYPOINT [ "bash" ]
