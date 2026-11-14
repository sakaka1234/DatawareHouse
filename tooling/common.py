"""Shared helpers for code that runs inside the agri-tools container."""

import json
import os
import subprocess
import time

HIVE_HOST = os.environ.get("HIVE_HOST", "hiveserver2")
HIVE_PORT = int(os.environ.get("HIVE_PORT", "10000"))
ARTIFACTS = os.environ.get("ARTIFACTS_DIR", "/opt/agri/artifacts")
REPORTS = os.environ.get("REPORTS_DIR", "/opt/agri/reports")
SPARK_MASTER = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")


def ensure_dirs(*paths):
    for path in paths:
        if path and not os.path.isdir(path):
            os.makedirs(path)


def artifacts_path(*parts):
    ensure_dirs(ARTIFACTS)
    return os.path.join(ARTIFACTS, *parts)


def reports_path(*parts):
    ensure_dirs(REPORTS)
    return os.path.join(REPORTS, *parts)


def connect(user="hive", database="agri_dwh"):
    from pyhive import hive

    connection = hive.Connection(
        host=HIVE_HOST,
        port=HIVE_PORT,
        username=user,
        database=database,
        auth="NOSASL",
    )
    if user == "hive":
        # SQL standard authorization does not make the admin role current by
        # itself; without this, the hive user cannot read Spark-created views.
        try:
            cursor = connection.cursor()
            cursor.execute("SET ROLE ADMIN")
            cursor.fetchall()
            cursor.close()
        except Exception:
            pass
    return connection


def query_df(sql, user="hive", database="agri_dwh"):
    import pandas as pd

    connection = connect(user=user, database=database)
    try:
        frame = pd.read_sql(sql, connection)
        # HiveServer2 sometimes prefixes result columns with the table name.
        frame.columns = [str(column).split(".")[-1] for column in frame.columns]
        return frame
    finally:
        connection.close()


def query_rows(sql, user="hive", database="agri_dwh", chunk=100000):
    """Yield result rows in chunks without loading the whole table."""
    connection = connect(user=user, database=database)
    try:
        cursor = connection.cursor()
        cursor.arraysize = chunk
        cursor.execute(sql)
        while True:
            rows = cursor.fetchmany(chunk)
            if not rows:
                break
            yield rows
    finally:
        connection.close()


def execute(sql, user="hive", database="agri_dwh"):
    connection = connect(user=user, database=database)
    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        return cursor.fetchall()
    finally:
        connection.close()


def docker_exec(container, command, detach=False, timeout=None):
    args = ["docker", "exec"]
    if detach:
        args.append("-d")
    args.append(container)
    if isinstance(command, str):
        args += ["bash", "-lc", command]
    else:
        args += command
    return subprocess.run(
        args, check=False, text=True, timeout=timeout, capture_output=True
    )


def spark_submit(job, job_args=None, packages=None, driver_memory="600m",
                 executor_memory="800m", timeout=3600, detach=False):
    """Submit a job to the standalone Spark cluster from inside spark-master."""
    command = [
        "/spark/bin/spark-submit",
        "--master", SPARK_MASTER,
        "--conf", "spark.driver.memory=" + driver_memory,
        "--conf", "spark.executor.memory=" + executor_memory,
        "--conf", "spark.executor.cores=1",
        "--conf", "spark.cores.max=2",
        "--conf", "spark.sql.shuffle.partitions=8",
        "--conf", "spark.driver.host=spark-master",
    ]
    if packages:
        command += ["--packages", packages]
    command.append("/opt/agri/spark/jobs/" + job)
    if job_args:
        command += list(job_args)
    return docker_exec("spark-master", command, detach=detach, timeout=timeout)


def wait_for(predicate, timeout=180, interval=3, label="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    raise TimeoutError("timed out waiting for " + label)


def write_json(path, payload):
    ensure_dirs(os.path.dirname(path))
    with open(path, "w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def read_json(path, default=None):
    try:
        with open(path) as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return default
