# Airflow image with the Docker CLI, so DAGs can drive the Spark cluster the
# same way a scheduler drives external compute in production.
FROM docker:27-cli AS dockercli

FROM apache/airflow:2.9.3-python3.11

USER root
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
RUN chmod 755 /usr/local/bin/docker

USER airflow
RUN pip install --no-cache-dir docker==7.1.0
