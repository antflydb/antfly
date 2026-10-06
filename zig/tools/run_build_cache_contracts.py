#!/usr/bin/env python3
# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
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
    selected = [case for case in all_cases if owners[case.id()] == shard]
    if not selected:
        raise RuntimeError(f"{shard}: no cache contracts selected")
    return unittest.TestSuite(selected)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("shard", choices=("runtime", "storage"))
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
