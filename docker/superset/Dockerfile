FROM apache/superset:4.0.2

USER root
RUN pip install --no-cache-dir \
    pyhive==0.7.0 \
    thrift==0.16.0 \
    pure-sasl==0.6.2 \
    thrift-sasl==0.4.3 \
    psycopg2-binary==2.9.9
USER superset
