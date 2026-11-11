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

"""Run the actual GCS HN qualification against the shaped loopback provider.

Uses the same corpus loader, overlay, highlights, restart, mutation assertions
and publication barrier. Startup is never drained before the cold query.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import importlib
import json
from pathlib import Path
import sys

from lake_network_store import Network, NetworkStore


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--corpus-dir", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--term", default="database")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--latency-ms", type=float, default=100)
    parser.add_argument("--jitter-ms", type=float, default=30)
    parser.add_argument("--connection-ms", type=float, default=50)
    parser.add_argument("--bandwidth-mib", type=float, default=16)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument(
        "--store-source", action=argparse.BooleanOptionalAction, default=True
    )
    args = parser.parse_args(argv)
    policy = Network(
        latency_ms=args.latency_ms,
        jitter_ms=args.jitter_ms,
        bandwidth_mib=args.bandwidth_mib,
        concurrency=args.concurrency,
        seed=1,
        connection_ms=args.connection_ms,
    )
    policy.validate()
    assert args.binary.is_file() and args.corpus_dir.is_dir()
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    root = Path(__file__).resolve().parents[2]
    sys.path.insert(0, str(root / "zig/e2e/antfly"))
    import pytest

    test = importlib.import_module("test_composed_lake_readers")
    report = {
        "binary_sha256": hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        "network": asdict(policy),
        "properties": {},
        "complete": False,
        "qualification": "same GCS composed fixture; loopback HTTP transport",
    }
    with (
        NetworkStore(out / "objects", policy, bucket="hn-loopback") as service,
        pytest.MonkeyPatch.context() as patch,
    ):
        token_file = out / "token"
        token_file.write_text("loopback-only")
        token_file.chmod(0o600)
        env = {
            "ANTFLY_BIN": str(args.binary.resolve()),
            "ANTFLY_E2E_GCS_BUCKET": service.store.bucket,
            "ANTFLY_E2E_GCS_PREFIX": "qualification/hn-matched-loopback",
            "ANTFLY_E2E_GCS_TOKEN_FILE": str(token_file),
            "ANTFLY_E2E_GCS_LOOPBACK_BASE": service.url,
            "ANTFLY_E2E_COMPOSED_CORPUS_DIR": str(args.corpus_dir.resolve()),
            "ANTFLY_E2E_COMPOSED_ARCHIVE_ROWS": str(args.rows),
            "ANTFLY_E2E_COMPOSED_PROFILE_TERM": args.term,
            "ANTFLY_E2E_COMPOSED_PROFILE_ROUNDS": "1",
            "ANTFLY_E2E_COMPOSED_HIGHLIGHTS": "1",
            "ANTFLY_E2E_COMPOSED_STORE_SOURCE": "1" if args.store_source else "0",
            "ANTFLY_E2E_COMPOSED_CACHE_MIB": "64",
            "ANTFLY_E2E_COMPOSED_PENDING_ROWS": "0",
            "ANTFLY_E2E_COMPOSED_CONCURRENT_WRITES": "1",
        }
        for key, value in env.items():
            patch.setenv(key, value)
        patch.setattr(
            test, "network_phase", lambda name: service.configure(policy, name)
        )
        try:
            test.test_composed_lake_preparation_reuses_pins_and_keeps_restart_cursors(
                patch,
                lambda name, value: report["properties"].__setitem__(name, value),
                "gcs_remote",
                "explicit",
                True,
            )
            assert (
                hashlib.sha256(args.binary.read_bytes()).hexdigest()
                == report["binary_sha256"]
            ), "Candidate binary changed during benchmark"
            report["complete"] = True
        finally:
            service.wait_idle()
            for phase in ("setup", "cold_startup", "cold_query", "after_cold_query"):
                report.setdefault("io", {})[phase] = service.summary(phase)
            # Preserve counters and hashes, not tokens or raw query/source data.
            (out / "report.json").write_text(json.dumps(report, indent=2))
            token_file.unlink()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
