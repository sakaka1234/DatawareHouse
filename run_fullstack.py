"""Execute notebooks 05-09 against the running Docker stack."""

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
NOTEBOOKS = [
    "05_full_stack_batch.ipynb",
    "06_kafka_streaming.ipynb",
    "07_airflow_quality_rbac.ipynb",
    "08_ml_on_hive.ipynb",
    "09_bi_benchmark_fault.ipynb",
]


def main():
    for name in NOTEBOOKS:
        path = ROOT / "notebooks" / name
        print("\n=== %s ===" % name, flush=True)
        started = time.perf_counter()
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "jupyter",
                "nbconvert",
                "--to",
                "notebook",
                "--execute",
                "--inplace",
                str(path),
                "--ExecutePreprocessor.timeout=1800",
            ],
            cwd=ROOT / "notebooks",
        )
        elapsed = time.perf_counter() - started
        if result.returncode != 0:
            print("FAILED after %.1fs: %s" % (elapsed, name))
            return result.returncode
        print("OK in %.1fs: %s" % (elapsed, name))
    print("\nFull-stack notebooks completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
