# base image is official "slim" Python 3.9 on Debian 11 ("bullseye")
FROM python:3.9-slim-bullseye

# Install libsodium and timezone data
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends libsodium-dev tzdata && \
    apt-get autoremove -y

# Install OpenMSIStream
RUN pip install --upgrade pip && \
    pip install openmsistream && \
    pip cache purge

# Set the timezone
ARG TZ="America/New_York"
ENV TZ=$TZ
