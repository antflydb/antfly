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

"""Fail fast when a qualification host cannot execute its local graph producer.

Uses public synthetic text and a fresh database. No model is downloaded, and a
missing model is a failure rather than a skipped qualification gate.
"""

import argparse
import hashlib
import json
import time
from pathlib import Path

from lite_contention import sdk


MODEL = "fastino/gliner2.5-base-v1"
TEXT = "John Smith works at Microsoft in Seattle."


def run(library, directory, history_windows=False):
    af = sdk(str(Path(library).resolve()))
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    database = directory / "database.aflite"
    started = time.monotonic()
    schema = {"entities": ["person", "organization"]}
    options = {
        "include_confidence": True,
        "include_spans": True,
        "long_document": {"mode": "window"},
    }
    window_cases = []
    # Standalone success alone does not test the database's producer wiring.
    with af.Inference.open(call_timeout_ms=120000) as inference:
        result = inference.extract(
            {
                "schema_version": 2,
                "model": MODEL,
                "schema": schema,
                "options": options,
                "inputs": [{"content": TEXT}],
            }
        )
        entities = result["data"][0]["entities"]
        for label, expected in (
            ("person", "John Smith"),
            ("organization", "Microsoft"),
        ):
            if not any(
                entity["label"] == label and entity["text"] == expected
                for entity in entities
            ):
                raise RuntimeError("Synthetic extraction did not find " + label)
        if history_windows:
            history_schema = {
                "entities": [
                    "concept",
                    "technology",
                    "tool",
                    "project",
                    "organization",
                    "person",
                    "goal",
                    "problem",
                    "practice",
                    "constraint",
                    "metric",
                ],
                "relations": [],
            }
            unit = "configuration visibility admission maintenance acknowledgement "
            for name, text in (
                ("short_final_window", unit * 205),
                ("composition_document", (unit * 3200)[:190000]),
            ):
                began = time.monotonic()
                result = inference.extract(
                    {
                        "schema_version": 2,
                        "model": MODEL,
                        "schema": history_schema,
                        "options": options,
                        "inputs": [{"content": text}],
                    }
                )
                windows = result["data"][0]["long_document"]["window_count"]
                if windows < 2:
                    raise RuntimeError(
                        "Long history fixture did not exercise windowing"
                    )
                window_cases.append(
                    {
                        "case": name,
                        "bytes": len(text.encode()),
                        "words": len(text.split()),
                        "windows": windows,
                        "seconds": time.monotonic() - began,
                    }
                )
    config = {
        "name": "knowledge",
        "type": "graph",
        "source": {
            "artifact": "kg",
            "format": "extraction_graph",
            "mention_edge_type": "mentions",
        },
        "artifact": {
            "name": "kg",
            "kind": "asset",
            "source": {"type": "field", "value": "body"},
            "content_type": "application/json",
            "producer_json": {
                "type": "extractor",
                "config": {
                    "provider": "antfly",
                    "model": MODEL,
                    "schema": history_schema if history_windows else schema,
                    "options": options,
                },
            },
        },
        "edge_types": [{"name": "mentions"}],
    }
    with af.create(
        database, local_runtime=True, generated_enrichment_replay=True, busy_timeout=5
    ) as db:
        db.add_index(config)
        inserts = {"example": {"body": TEXT}}
        if history_windows:
            inserts["history-tail"] = {"body": unit * 205}
            inserts["history-composition"] = {"body": (unit * 3200)[:190000]}
        db.batch_json({"inserts": inserts, "sync_level": "write"})
        db.run_until_idle()
        stats = db.stats()
        enrichment = stats["enrichment"]
        if enrichment["processed_requests"] < len(inserts) or enrichment["error_count"]:
            raise RuntimeError("Database extractor did not complete cleanly")
        graph = next(
            index for index in stats["indexes"] if index["name"] == "knowledge"
        )
        if graph["coverage_produced_count"] != len(inserts):
            raise RuntimeError("Database graph producer did not publish its result")
        for index in stats["indexes"]:
            if index["replay_catch_up_required"] or (
                index["replay_applied_sequence"] < index["replay_target_sequence"]
            ):
                raise RuntimeError("Database index replay did not catch up")
        if not db.check()["valid"]:
            raise RuntimeError("Preflight database failed integrity")
    with af.open_readonly(database) as db:
        if (
            any(
                db.lookup(key)["body"] != value["body"]
                for key, value in inserts.items()
            )
            or not db.check()["valid"]
        ):
            raise RuntimeError("Preflight database failed reopen verification")
    with Path(library).open("rb") as stream:
        library_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
    return {
        "passed": True,
        "model": MODEL,
        "library_sha256": library_sha256,
        "standalone_entities_verified": True,
        "history_window_cases": window_cases,
        "database_processed_requests": enrichment["processed_requests"],
        "database_errors": enrichment["error_count"],
        "graph_coverage_produced": graph["coverage_produced_count"],
        "index_catchup_integrity_reopen": True,
        "seconds": time.monotonic() - started,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True)
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument(
        "--history-windows",
        action="store_true",
        help="also require bounded multi-window history extraction",
    )
    args = parser.parse_args()
    report = run(args.library, args.directory, args.history_windows)
    (args.directory / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)


if __name__ == "__main__":
    main()
