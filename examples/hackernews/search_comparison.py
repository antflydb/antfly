#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Measure prebuilt DuckDB BM25 and Parquet scans without conflating cache states.

Each cycle is a fresh process. OS file caches are deliberately not flushed.
Extension installation and index construction are excluded from query latency.
Remote scans are optional, unranked substring searches, not an FTS equivalent.
"""

import argparse
import hashlib
import json
import math
import platform
from pathlib import Path
import subprocess
import sys
import time


FIELDS = "hn_id, title, item_type, author, points, created_at"


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def percentile(samples):
    return sorted(samples)[math.ceil(len(samples) * 0.95) - 1]


def connect(database, threads):
    import duckdb

    return duckdb.connect(str(database), config={"threads": threads})


def sample(connection, sql, parameters):
    started = time.perf_counter()
    rows = connection.execute(sql, parameters).fetchall()
    return round((time.perf_counter() - started) * 1000, 2), rows


def worker(args):
    import duckdb

    connection = connect(
        args.database if args.mode == "fts" else ":memory:", args.threads
    )
    if args.mode == "fts":
        connection.execute("LOAD fts")
        sql = f"""SELECT {FIELDS}, score FROM (
            SELECT *, fts_main_documents.match_bm25(hn_id, ?, fields := 'body') AS score
            FROM documents) WHERE score IS NOT NULL
            ORDER BY score DESC, hn_id LIMIT 5"""
    else:
        if args.mode == "remote-scan":
            connection.execute("LOAD httpfs")
            if args.bearer_stdin:
                token = sys.stdin.readline(65537).strip()
                if not token or len(token) > 65536:
                    raise ValueError("Expected bounded bearer credential on stdin")
                # Temporary in-memory secret; never persist credentials or source URLs.
                from urllib.parse import urlsplit

                scope = urlsplit(args.source)
                connection.execute(
                    "CREATE TEMPORARY SECRET comparison_http (TYPE HTTP, "
                    + "BEARER_TOKEN "
                    + literal(token)
                    + ", SCOPE "
                    + literal(scope.scheme + "://" + scope.netloc + "/")
                    + ")"
                )
        sql = f"""SELECT {FIELDS} FROM read_parquet(?)
            WHERE contains(lower(body), lower(?)) ORDER BY hn_id LIMIT 5"""
    workloads = {}
    for term in args.terms:
        parameters = [term] if args.mode == "fts" else [args.source, term]
        first, rows = sample(connection, sql, parameters)
        samples = []
        for _ in range(args.repeats):
            elapsed, repeated = sample(connection, sql, parameters)
            if repeated != rows:
                raise ValueError("Repeated query changed ordered results")
            samples.append(elapsed)
        workloads[term] = {
            "first_ms": first,
            "warm_samples_ms": samples,
            "warm_p95_ms": percentile(samples),
            "top_hn_ids": [row[0] for row in rows],
            "scores": [row[-1] for row in rows] if args.mode == "fts" else None,
        }
    row_count = connection.execute(
        "SELECT count(*) FROM documents"
        if args.mode == "fts"
        else "SELECT count(*) FROM read_parquet(?)",
        [] if args.mode == "fts" else [args.source],
    ).fetchone()[0]
    if row_count != args.expected_rows:
        raise ValueError("Unexpected source row count")
    connection.close()
    return {
        "mode": args.mode,
        "duckdb_version": duckdb.__version__,
        "platform": platform.platform(),
        "architecture": platform.machine(),
        "threads": args.threads,
        "rows": row_count,
        "workloads": workloads,
        "cache_state": "fresh process; OS file cache uncontrolled; each term follows preceding terms",
        "ranking": "BM25"
        if args.mode == "fts"
        else "none; substring scan ordered by hn_id",
        "highlights": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", required=True, help="Local Parquet or HTTPS URL for remote-scan"
    )
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=("fts", "scan", "remote-scan"), default="fts")
    parser.add_argument(
        "--terms", nargs="+", default=["database", "postgres", "sqlite", "AI"]
    )
    parser.add_argument("--repeats", type=int, default=20)
    parser.add_argument("--cycles", type=int, default=2)
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--expected-rows", type=int, default=142577)
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--bearer-stdin", action="store_true")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if min(args.repeats, args.cycles, args.threads, args.expected_rows) < 1:
        parser.error("Counts must be positive")
    if args.cycles > 10 or args.repeats > 100:
        parser.error("At most 10 cycles and 100 repeats")
    if args.mode == "remote-scan" and (
        args.build or not args.source.startswith("https://")
    ):
        parser.error("Remote mode requires HTTPS and cannot build an index")
    if args.worker:
        print(json.dumps(worker(args)))
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    build = None
    source_digest = (
        hashlib.sha256(Path(args.source).read_bytes()).hexdigest()
        if args.mode != "remote-scan"
        else None
    )
    if args.mode == "fts":
        if args.build:
            if args.database.exists():
                parser.error("Build requires a new database path")
            args.database.parent.mkdir(parents=True, exist_ok=True)
            connection = connect(args.database, args.threads)
            connection.execute("INSTALL fts")
            connection.execute("LOAD fts")
            started = time.perf_counter()
            connection.execute(
                "CREATE TABLE documents AS SELECT * FROM read_parquet(?)", [args.source]
            )
            rows = connection.execute(
                "SELECT count(*), count(DISTINCT hn_id) FROM documents"
            ).fetchone()
            if rows != (args.expected_rows, args.expected_rows):
                raise ValueError("Unexpected row count or duplicate HN identifiers")
            connection.execute("PRAGMA create_fts_index('documents', 'hn_id', 'body')")
            connection.execute(
                "CREATE TABLE comparison_provenance (source_sha256 VARCHAR, rows BIGINT)"
            )
            connection.execute(
                "INSERT INTO comparison_provenance VALUES (?, ?)",
                [source_digest, rows[0]],
            )
            connection.execute("CHECKPOINT")
            connection.close()
            build = {
                "seconds": round(time.perf_counter() - started, 3),
                "rows": rows[0],
            }
        elif not args.database.exists():
            parser.error("Prebuilt database missing; use --build once")
        else:
            connection = connect(args.database, args.threads)
            provenance = connection.execute(
                "SELECT source_sha256, rows FROM comparison_provenance"
            ).fetchall()
            connection.close()
            if provenance != [(source_digest, args.expected_rows)]:
                raise ValueError("Prebuilt index belongs to another source inventory")
    elif args.mode == "remote-scan":
        connection = connect(":memory:", args.threads)
        connection.execute("INSTALL httpfs")
        connection.close()
    token = sys.stdin.readline(65537).strip() if args.bearer_stdin else None
    if token is not None and (not token or len(token) > 65536):
        parser.error("Expected bounded bearer credential on stdin")
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        "--source",
        args.source,
        "--database",
        str(args.database),
        "--output",
        str(args.output),
        "--mode",
        args.mode,
        "--repeats",
        str(args.repeats),
        "--threads",
        str(args.threads),
        "--expected-rows",
        str(args.expected_rows),
        "--terms",
        *args.terms,
    ]
    if token is not None:
        command.append("--bearer-stdin")
    cycles = []
    for _ in range(args.cycles):
        result = subprocess.run(
            command,
            input=None if token is None else token + "\n",
            text=True,
            capture_output=True,
            timeout=600,
        )
        if result.returncode:
            # Provider/SQL errors may include credentials or private source URLs.
            raise RuntimeError(
                "Comparison worker failed; exit code " + str(result.returncode)
            )
        cycles.append(json.loads(result.stdout))
    report = {
        "kind": "duckdb_search_comparison",
        "build": build,
        "cycles": cycles,
        "source_sha256": source_digest,
        "fts_analysis": "DuckDB defaults: porter stemming, English stopwords; tokenization/scores may differ from Antfly",
        "limitations": [
            "No highlights; Antfly qualification includes highlights and HTTP overhead.",
            "Restart is process-cold, not a flushed OS file cache.",
            "Remote scans have no ranked text index; not equivalent to BM25.",
            "First-query latency excludes connection and extension loading.",
        ],
    }
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(
        json.dumps(
            {"mode": args.mode, "cycles": len(cycles), "output": str(args.output)}
        )
    )


if __name__ == "__main__":
    main()
