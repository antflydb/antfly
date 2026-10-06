# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Keep the cache-contract CI shards exhaustive and disjoint."""

import unittest

from tools import run_build_cache_contracts as routing


class CacheContractShardTests(unittest.TestCase):
    def test_every_contract_has_exactly_one_shard(self):
        discovered = {
            case.id()
            for case in routing.cases(
                unittest.TestLoader().loadTestsFromNames(
                    ["tools.test_runtime_cache", "tools.test_linked_tests"]
                )
            )
        }
        runtime = {case.id() for case in routing.cases(routing.select("runtime"))}
        storage = {case.id() for case in routing.cases(routing.select("storage"))}
        self.assertFalse(runtime & storage)
        self.assertEqual(runtime | storage, discovered)
        self.assertIn(routing.HOST_GENERATOR, storage)

    def test_runtime_ci_shards_cover_every_runtime_contract_once(self):
        runtime = {case.id() for case in routing.cases(routing.select("runtime"))}
        first = {case.id() for case in routing.cases(routing.select("runtime-1"))}
        second = {case.id() for case in routing.cases(routing.select("runtime-2"))}
        self.assertFalse(first & second)
        self.assertEqual(first | second, runtime)
        self.assertLessEqual(abs(len(first) - len(second)), 1)
        self.assertNotIn(routing.HOST_GENERATOR, first | second)


if __name__ == "__main__":
    unittest.main()
