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


if __name__ == "__main__":
    unittest.main()
