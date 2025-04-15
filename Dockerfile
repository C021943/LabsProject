FROM bitnami/spark:3.5

# Delta 2.4.0 compatible with Spark 3.5
ENV DELTA_VERSION=2.4.0
ENV SCALA_VERSION=2.12

# Descargar JARs necesarios para Delta
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar https://repo1.maven.org/maven2/io/delta/delta-spark_${SCALA_VERSION}/${DELTA_VERSION}/delta-spark_${SCALA_VERSION}-${DELTA_VERSION}.jar

WORKDIR /workspace