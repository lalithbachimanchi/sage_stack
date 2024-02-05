# Use Ubuntu 20.04 as the base image
FROM ubuntu:20.04

#  system dependencies
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install  -y \
    python3 \
    python3-pip \
    openjdk-11-jdk \
    wget \
    vim \
    && apt-get clean

# Install necessary system libraries (if required)
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    python3-dev \
    build-essential \
    libffi-dev \
    libssl-dev \
    mysql-client \
    pkg-config \
    libssl1.1 \
    && rm -rf /var/lib/apt/lists/*


COPY requirements.txt /opt/requirements.txt

RUN pip install "apache-airflow[celery]==2.7.2" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.7.2/constraints-3.8.txt"

RUN pip install --no-cache-dir -r /opt/requirements.txt

RUN wget -O /opt/spark-3.5.0-bin-hadoop3.tgz https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz


RUN tar -xzf /opt/spark-3.5.0-bin-hadoop3.tgz -C /opt && \
    ln -s /opt/spark-3.5.0-bin-hadoop3 /opt/spark

COPY spark/ /opt/spark/

COPY data/ /opt/data

COPY great_expectations /opt/great_expectations/

COPY table_mapping_json.json /opt/table_mapping_json.json

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV AIRFLOW_HOME=/opt/airflow
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False

# Expose Airflow webserver and scheduler ports
EXPOSE 8080 8793

# Copy the Airflow configuration file
COPY dags /opt/airflow/dags/

# Start Airflow scheduler and webserver
COPY entrypoint.sh /entrypoint.sh
# Make the script executable
RUN chmod +x /entrypoint.sh

CMD ["/entrypoint.sh"]
# CMD ["airflow", "scheduler", "&&", "airflow", "webserver"]
