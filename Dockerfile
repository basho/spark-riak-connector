# Oracle JDK 8 image based on Alpite Linux
FROM frolvlad/alpine-oraclejdk8

MAINTAINER Alexey Suprun <asuprun@contractor.basho.com>

# These options could be changed during starting container using --build-arg property with folliwing syntax:
# --build-arg ARGUMENT_NAME=value
ARG SBT_VERSION=0.13.12
ARG SPARK_VERSION=1.6.1
ARG SPARK_HADOOP_VERSION=hadoop2.6

# Set env vars
ENV SBT_HOME /usr/local/sbt
ENV SPARK_HOME /usr/local/spark
ENV PATH $PATH:$SPARK_HOME/bin:$SBT_HOME/bin

# Install general dependencies
RUN apk add --no-cache curl bash python jq

# Install SBT
RUN curl -q -sSL --retry 3 "http://dl.bintray.com/sbt/native-packages/sbt/$SBT_VERSION/sbt-$SBT_VERSION.tgz" | tar -xz -C /usr/local

# Install Spark
RUN curl -q -sSL --retry 3 "http://www-eu.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-$SPARK_HADOOP_VERSION.tgz" | tar -xz -C /usr/local && \
    ln -s /usr/local/spark-$SPARK_VERSION-bin-$SPARK_HADOOP_VERSION $SPARK_HOME

# Install test dependencies for Python
RUN python -m ensurepip && \
    rm -r /usr/lib/python*/ensurepip && \
    pip install --upgrade pip setuptools pytest py4j findspark riak timeout_decorator tzlocal && \
    rm -r /root/.cache

CMD ["/bin/bash"]
