# L'image de base est fournie par docker-compose via la variable AIRFLOW_IMAGE_NAME
ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.0.6
FROM ${AIRFLOW_IMAGE_NAME}

USER airflow
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
