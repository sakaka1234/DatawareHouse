#!/bin/bash
set -e

sed -i 's/export HADOOP_CLIENT_OPTS= /export HADOOP_CLIENT_OPTS=/' /opt/hive/bin/ext/*.sh

mkdir -p /tmp/hive /tmp/hive_resources /tmp/hive_querylog
chmod -R 777 /tmp/hive /tmp/hive_resources /tmp/hive_querylog

exec /opt/hive/bin/hive --service hiveserver2
