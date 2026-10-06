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

"""Run complete, disjoint cache-contract shards with balanced CI runtimes."""

from __future__ import annotations

import argparse
import sys
import unittest

HOST_GENERATOR = (
    "tools.test_runtime_cache.RuntimeCacheTest.test_host_generator_cache_contracts"
)


def cases(suite: unittest.TestSuite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from cases(item)
        else:
            yield item


def select(shard: str) -> unittest.TestSuite:
    loader = unittest.TestLoader()
    all_cases = list(
        cases(
            loader.loadTestsFromNames(
                ["tools.test_runtime_cache", "tools.test_linked_tests"]
            )
        )
    )
    if loader.errors:
        raise RuntimeError("failed to discover cache contracts: " + str(loader.errors))
    if not any(case.id() == HOST_GENERATOR for case in all_cases):
        raise RuntimeError("host generator cache contract was not discovered")
    owners = {}
    for case in all_cases:
        name = case.id()
        if name == HOST_GENERATOR or name.startswith("tools.test_linked_tests."):
            owners[name] = "storage"
        elif name.startswith("tools.test_runtime_cache."):
            owners[name] = "runtime"
        else:
            raise RuntimeError(f"unassigned cache contract: {name}")
    if len(owners) != len(all_cases):
        raise RuntimeError("duplicate cache contract discovered")
    owner = "runtime" if shard in {"runtime-1", "runtime-2"} else shard
    selected = [case for case in all_cases if owners[case.id()] == owner]
    if shard in {"runtime-1", "runtime-2"}:
        # Interleave the ordered runtime contracts so expensive native builds
        # and checkpoint fixtures are spread across both runners.
        selected = selected[int(shard[-1]) - 1 :: 2]
    if not selected:
        raise RuntimeError(f"{shard}: no cache contracts selected")
    return unittest.TestSuite(selected)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "shard", choices=("runtime", "runtime-1", "runtime-2", "storage")
    )
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    suite = select(args.shard)
    if args.list:
        for case in cases(suite):
            print(case.id())
        return
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        sys.exit(1)


if __name__ == "__main__":
    main()
