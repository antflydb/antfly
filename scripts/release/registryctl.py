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

"""Operate release registries with fail-closed lookup semantics."""

from __future__ import annotations

import argparse

from registry.container import (
    check_version,
    ensure_version,
    optional_digest,
    promote_alias,
    require_digest,
    verify_digest,
)
from registry.model import RegistryError
from registry.npm import dist_tag, version_integrity


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    for name in ("npm-integrity", "npm-tag"):
        command = subparsers.add_parser(name)
        command.add_argument("--package", required=True)
        if name == "npm-integrity":
            command.add_argument("--version", required=True)
        else:
            command.add_argument("--tag", required=True)

    command = subparsers.add_parser("container-alias")
    command.add_argument("--source", required=True)
    command.add_argument("--destination", required=True)

    command = subparsers.add_parser("container-version")
    command.add_argument("--source", required=True)
    command.add_argument("--destination", required=True)

    command = subparsers.add_parser("container-version-check")
    command.add_argument("--source", required=True)
    command.add_argument("--destination", required=True)

    command = subparsers.add_parser("container-digest")
    command.add_argument("--ref", required=True)

    command = subparsers.add_parser("container-lookup")
    command.add_argument("--ref", required=True)

    command = subparsers.add_parser("container-verify")
    command.add_argument("--ref", required=True)
    command.add_argument("--digest", required=True)

    args = parser.parse_args()
    try:
        if args.command == "npm-integrity":
            print(version_integrity(args.package, args.version) or "")
        elif args.command == "npm-tag":
            print(dist_tag(args.package, args.tag) or "")
        elif args.command == "container-alias":
            print(promote_alias(args.source, args.destination))
        elif args.command == "container-version":
            print(ensure_version(args.source, args.destination))
        elif args.command == "container-version-check":
            print(check_version(args.source, args.destination))
        elif args.command == "container-digest":
            print(require_digest(args.ref))
        elif args.command == "container-lookup":
            print(optional_digest(args.ref) or "")
        else:
            print(verify_digest(args.ref, args.digest))
    except RegistryError as exc:
        raise SystemExit(str(exc)) from exc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
