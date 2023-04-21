# base image is official "slim" Python 3.9 on Debian 11 ("bullseye")
FROM python:3.9-slim-bullseye
LABEL maintainer="OpenMSI development team <openmsistream@gmail.com>"

# Install sudo
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends sudo && \
    apt-get autoremove -y

# Create the openmsi group and user (with password-less sudo permissions)
RUN groupadd -r openmsi -g 1000 && \
    useradd -u 1000 -r -g openmsi -G sudo -m -d /home/openmsi -c "OpenMSI user" openmsi && \
    sed -i /etc/sudoers -re 's/^%sudo.*/%sudo ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^root.*/root ALL=(ALL:ALL) NOPASSWD: ALL/g' && \
    sed -i /etc/sudoers -re 's/^#includedir.*/## **Removed the include directive** ##"/g' && \
    echo "openmsi ALL=(ALL) NOPASSWD: ALL" >> /etc/sudoers && \
    chmod 775 /home/openmsi

# Run from the openmsi user's home directory, as the openmsi user
WORKDIR /home/openmsi
USER openmsi

# add the local binary directory to PATH
ENV PATH="/home/openmsi/.local/bin:$PATH"

# Install libsodium and timezone data
RUN sudo apt-get update && \
    sudo apt-get upgrade -y && \
    sudo apt-get install -y --no-install-recommends libsodium-dev tzdata && \
    sudo apt-get autoremove -y

# Install OpenMSIStream
RUN pip install --upgrade pip && \
    pip install openmsistream && \
    pip cache purge

# Upgrade to latest version
RUN pip install --upgrade openmsistream && \
    pip cache purge

# Set the timezone
ARG TZ="America/New_York"
ENV TZ=$TZ

# Use a bash Entrypoint
ENTRYPOINT [ "bash" ]
