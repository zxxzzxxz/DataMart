"""Run isolated PostgreSQL acceptance with an existing DSN or portable PG binaries.

No production schema is touched: tests create their own randomly named database.
--pg-bin starts a temporary loopback-only server and stops it even on test failure.
"""

import argparse
import os
import socket
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pg-bin", type=Path, help="Directory containing initdb/pg_ctl/postgres binaries")
    parser.add_argument("--output", type=Path, default=ROOT / "logs/acceptance/postgres.xml")
    parser.add_argument(
        "--all", action="store_true", help="Include DAG/API/unit regression with PostgreSQL acceptance"
    )
    args = parser.parse_args()
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    server_started = False
    data = None
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    try:
        if args.pg_bin:
            binary = args.pg_bin.resolve()
            if not (binary / ("initdb.exe" if os.name == "nt" else "initdb")).is_file():
                parser.error("--pg-bin does not contain initdb")
            # Never reuse an existing cluster or modify a system service.
            # Windows initdb drops administrator privileges. Python 3.13 mkdtemp's
            # restrictive ACL blocks that token; inherit the user's TEMP ACL instead.
            runtime = Path(tempfile.gettempdir()) / f"datamart-acceptance-{uuid.uuid4().hex}"
            runtime.mkdir()
            data = runtime / "data"
            suffix = ".exe" if os.name == "nt" else ""
            initialized = subprocess.run(
                [
                    str(binary / f"initdb{suffix}"),
                    "-D",
                    str(data),
                    "-U",
                    "postgres",
                    "-A",
                    "trust",
                    "-E",
                    "UTF8",
                    "--locale=C",
                ],
                capture_output=True,
                creationflags=flags,
            )
            if initialized.returncode:
                raise RuntimeError(
                    initialized.stderr.decode(errors="replace") + initialized.stdout.decode(errors="replace")
                )
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            # A Windows server can inherit pg_ctl's PIPE handles. Redirect to a
            # file so communicate() cannot wait for the long-lived server to exit.
            with (runtime / "control.log").open("w") as control:
                started = subprocess.run(
                    [
                        str(binary / f"pg_ctl{suffix}"),
                        "-D",
                        str(data),
                        "-l",
                        str(runtime / "postgres.log"),
                        "-o",
                        f"-p {port} -h 127.0.0.1",
                        "-w",
                        "-t",
                        "30",
                        "start",
                    ],
                    stdout=control,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    creationflags=flags,
                )
            if started.returncode:
                raise RuntimeError(
                    f"Temporary PostgreSQL failed to start; inspect {runtime / 'postgres.log'}"
                )
            server_started = True
            env["DATAMART_TEST_DSN"] = f"host=127.0.0.1 port={port} dbname=postgres user=postgres"
            psycopg2.connect(env["DATAMART_TEST_DSN"], connect_timeout=5).close()
            print(f"Temporary PostgreSQL ready; runtime: {runtime}", flush=True)
        elif not env.get("DATAMART_TEST_DSN"):
            parser.error("Set DATAMART_TEST_DSN or provide --pg-bin; acceptance must not silently skip")
        output = args.output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        if args.all:
            airflow_test = Path(tempfile.gettempdir()) / f"datamart-airflow-test-{uuid.uuid4().hex}"
            env["AIRFLOW_HOME"] = str(airflow_test)
            env["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = f"sqlite:///{airflow_test.as_posix()}/airflow.db"
            env["AIRFLOW__CORE__PLUGINS_FOLDER"] = str(ROOT / "plugins")
            env["AIRFLOW__CORE__LOAD_EXAMPLES"] = "false"
        return subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "tests" if args.all else "tests/test_warehouse_pipeline.py",
                "-q",
                "-p",
                "no:cacheprovider",
                "-o",
                "junit_family=xunit1",
                f"--junitxml={output}",
            ],
            cwd=ROOT,
            env=env,
        ).returncode
    finally:
        if server_started:
            subprocess.run(
                [str(binary / f"pg_ctl{suffix}"), "-D", str(data), "-m", "fast", "-w", "stop"],
                capture_output=True,
                timeout=30,
                creationflags=flags,
                check=True,
            )


if __name__ == "__main__":
    sys.exit(main())
