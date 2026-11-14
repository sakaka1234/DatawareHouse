#!/bin/bash
# The bde2020 entrypoint expands "$@" unquoted, so a command containing spaces
# is split. This script keeps the real command in a file instead.
set -e

# The bundled Hive 2.3 launcher has a stray space in
# "export HADOOP_CLIENT_OPTS= ..."; remove it before starting.
sed -i 's/export HADOOP_CLIENT_OPTS= /export HADOOP_CLIENT_OPTS=/' /opt/hive/bin/ext/*.sh

/opt/hive/bin/schematool -dbType postgres -initSchema || true
exec /opt/hive/bin/hive --service metastore
