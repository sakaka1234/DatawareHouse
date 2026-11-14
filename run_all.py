"""Run the five notebooks in order, failing fast if one breaks."""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
NOTEBOOKS = [
    "00_setup_and_profile.ipynb",
    "01_batch_dwh.ipynb",
    "02_streaming_simulation.ipynb",
    "03_ml_models.ipynb",
    "04_dashboard_and_fault_tests.ipynb",
]


def main() -> int:
    for name in NOTEBOOKS:
        path = ROOT / "notebooks" / name
        if not path.exists():
            print(f"Missing notebook: {path}")
            return 1
        print(f"\n=== {name} ===")
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
                "--ExecutePreprocessor.timeout=900",
            ],
            cwd=ROOT / "notebooks",
        )
        elapsed = time.perf_counter() - started
        if result.returncode != 0:
            print(f"FAILED after {elapsed:.1f}s: {name}")
            return result.returncode
        print(f"OK in {elapsed:.1f}s: {name}")
    print("\nAll notebooks completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
