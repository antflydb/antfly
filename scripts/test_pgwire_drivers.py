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

"""Real driver conformance; supply --binary to start an isolated authenticated node.

uv run --no-project --with requests --with pytest --with 'psycopg[binary]==3.3.6' \
  --with asyncpg==0.31.0 python scripts/test_pgwire_drivers.py --binary zig/zig-out/bin/antfly
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CLIENTS = ROOT / "scripts" / "pgwire_drivers"


def run_clients(url, drivers):
    env = {**os.environ, "ANTFLY_PGWIRE_URL": url}
    commands = {
        "python": [(CLIENTS, [sys.executable, "python.py"])],
        "node": [
            (CLIENTS, ["npm", "ci", "--ignore-scripts"]),
            (CLIENTS, ["node", "node.mjs"]),
        ],
        "go": [(CLIENTS / "go", ["go", "run", "."])],
        "rust": [(CLIENTS / "rust", ["cargo", "run", "--locked"])],
    }
    for driver in drivers:
        for cwd, command in commands[driver]:
            subprocess.run(command, cwd=cwd, env=env, check=True, timeout=300)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--binary", type=Path)
    target.add_argument(
        "--url",
        help="PostgreSQL connection URL for an existing authenticated Antfly node",
    )
    parser.add_argument(
        "--drivers",
        nargs="+",
        choices=["python", "node", "go", "rust"],
        default=["python", "node", "go", "rust"],
    )
    args = parser.parse_args()
    if args.url:
        run_clients(args.url, args.drivers)
        return
    sys.path.insert(0, str(ROOT / "zig" / "e2e" / "antfly"))
    from conftest import AUTH_BOOTSTRAP_PASSWORD, StandaloneAntflyServer

    server = StandaloneAntflyServer(
        str(args.binary.resolve()), "127.0.0.1", 0, pgwire=True
    )
    failed = False
    try:
        run_clients(
            f"postgres://admin:{AUTH_BOOTSTRAP_PASSWORD}@127.0.0.1:{server.pgwire_port}/antfly",
            args.drivers,
        )
    except BaseException:
        failed = True
        print(server.log_path.read_text()[-16000:], file=sys.stderr)
        raise
    finally:
        server.stop(test_failed=failed)


if __name__ == "__main__":
    main()
