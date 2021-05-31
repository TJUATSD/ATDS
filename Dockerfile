FROM ubuntu:18.04

USER root

# Get g++
RUN apt-get update -q && apt-get install -q -y --no-install-recommends \
    software-properties-common \
    build-essential \
    wget \
    g++ \
    clang \ 
    libmysqlclient-dev \
    libyaml-cpp-dev



WORKDIR /app

COPY . .

RUN cd /app/src && \
    make run