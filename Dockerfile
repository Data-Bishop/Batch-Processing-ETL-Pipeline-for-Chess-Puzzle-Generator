FROM python:3.10-bullseye

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-11-jdk \
        sudo \
        curl \
        bash \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and install Spark
ARG SPARK_VERSION=3.5.1
RUN curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && mkdir -p /opt/spark \
    && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
    && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . ./app

CMD ["spark-submit", "etl/transform_test1.py"]