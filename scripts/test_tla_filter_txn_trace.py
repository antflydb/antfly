"""Regression checks for causal projection of transaction TLA traces."""

import json
import subprocess
import sys
import unittest
from pathlib import Path


FILTER = Path(__file__).with_name("tla-filter-txn-trace.py")


def event(txn_id, name, keys=()):
    return {
        "tag": "antfly-trace",
        "event": {
            "txnId": txn_id,
            "name": name,
            "shardId": "local",
            "state": {"writeKeys": list(keys), "deleteKeys": [], "predicateKeys": []},
        },
    }


class TransactionTraceFilterTests(unittest.TestCase):
    def run_filter(self, events):
        return subprocess.run(
            [sys.executable, str(FILTER)],
            input="".join(json.dumps(item) + "\n" for item in events),
            text=True,
            capture_output=True,
        )

    def test_dropped_transaction_taints_its_entire_key_component(self):
        events = [
            event("bad", "InitTransaction"),
            event("bad", "InitTransaction"),  # multi-shard lifecycle
            event("bad", "WriteIntentOnShard", ["shared"]),
            event("dependent", "InitTransaction"),
            event("dependent", "CheckPredicates"),
            event("dependent", "WriteIntentOnShard", ["shared", "bridge"]),
            event("transitive", "InitTransaction"),
            event("transitive", "CheckPredicates"),
            event("transitive", "WriteIntentOnShard", ["bridge"]),
            event("independent", "InitTransaction"),
            event("independent", "CheckPredicates"),
            event("independent", "WriteIntentOnShard", ["safe"]),
        ]
        result = self.run_filter(events)
        self.assertEqual(result.returncode, 0, result.stderr)
        retained = [json.loads(line)["event"] for line in result.stdout.splitlines()]
        self.assertEqual([item["txnId"] for item in retained], ["independent"] * 3)

    def test_malformed_json_still_fails(self):
        result = subprocess.run(
            [sys.executable, str(FILTER)],
            input='{"tag":"antfly-trace","event":{"txnId":"broken\x00"}}\n',
            text=True,
            capture_output=True,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("JSONDecodeError", result.stderr)


if __name__ == "__main__":
    unittest.main()
