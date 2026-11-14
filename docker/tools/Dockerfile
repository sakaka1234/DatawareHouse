# Python toolbox used by the producer, ML jobs, benchmark driver and quality gate.
# The Docker CLI lets it submit Spark jobs to the cluster, the same way an
# orchestration container drives external compute.
FROM docker:27-cli AS dockercli

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker

RUN pip install \
    pandas==2.2.3 \
    numpy==1.26.4 \
    pyarrow==17.0.0 \
    scikit-learn==1.5.1 \
    statsmodels==0.14.2 \
    joblib==1.4.2 \
    kafka-python==2.1.5 \
    pyhive==0.7.0 \
    thrift==0.16.0 \
    pure-sasl==0.6.2 \
    thrift-sasl==0.4.3 \
    requests==2.32.3 \
    prometheus-client==0.21.0

WORKDIR /opt/agri
CMD ["sleep", "infinity"]
