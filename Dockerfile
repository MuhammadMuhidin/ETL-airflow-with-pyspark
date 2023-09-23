FROM apache/airflow:2.7.0

USER root
RUN apt-get update \
    && apt-get install -y openjdk-11-jdk procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
# Set PATH to include JAVA_HOME/bin
ENV PATH $JAVA_HOME/bin:$PATH

USER airflow
COPY requirements.txt .
RUN pip install -r requirements.txt