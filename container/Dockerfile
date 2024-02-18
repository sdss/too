FROM python:3.12-slim-bullseye

MAINTAINER Jose Sanchez-Gallego, gallegoj@uw.edu
LABEL org.opencontainers.image.source https://github.com/sdss/lvmnps

WORKDIR /opt

RUN apt update && apt install -y git build-essential

COPY . too

RUN pip3 install -U pip setuptools wheel
RUN cd too && pip3 install .

RUN rm -Rf too
