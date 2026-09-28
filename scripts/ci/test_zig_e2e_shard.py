import json
import shlex
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import zig_e2e_shard as shards
from merge_e2e_durations import merge
from summarize_e2e_timings import summarize


def item(name, distributed=False):
    return SimpleNamespace(
        nodeid=name,
        fixturenames=[shards.DISTRIBUTED_FIXTURE] if distributed else ["backup_api"],
    )


def config(shard, plan=None):
    values = {
        "antfly_ci_shard": shard,
        "antfly_ci_plan": plan,
        "antfly_ci_plan_output": None,
    }
    return SimpleNamespace(
        getoption=lambda name, default=None: values.get(name, default),
        hook=SimpleNamespace(pytest_deselected=lambda **kw: None),
    )


class ShardTests(unittest.TestCase):
    def test_balanced_partition_preserves_groups_and_is_order_independent(self):
        rows = [
            (
                f"test_a.py::test_{i}",
                f"group-{i // 2}",
                "ordinary" if i < 12 else "recovery",
                float(i + 1),
            )
            for i in range(24)
        ]
        plan = shards.balance_records(rows)
        self.assertEqual(plan, shards.balance_records(list(reversed(rows))))
        for i in range(0, 24, 2):
            self.assertEqual(
                plan["assignments"][rows[i][0]], plan["assignments"][rows[i + 1][0]]
            )
        self.assertEqual(len(plan["assignments"]), 24)
        self.assertEqual(
            set(plan["assignments"].values()),
            {f"{family}-{i}" for family in ("ordinary", "recovery") for i in range(3)},
        )
        entries = [item(row[0], row[2] == "recovery") for row in rows]
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "plan.json"
            path.write_text(json.dumps(plan))
            selected = []
            for shard in set(plan["assignments"].values()):
                lane = list(reversed(entries))
                shards.pytest_collection_modifyitems(config(shard, str(path)), lane)
                selected.extend(i.nodeid for i in lane)
            self.assertEqual(sorted(selected), sorted(i.nodeid for i in entries))
            self.assertEqual(len(selected), len(set(selected)))
            with self.assertRaisesRegex(ValueError, "collection differs"):
                shards.pytest_collection_modifyitems(
                    config("ordinary-0", str(path)), entries[:-1]
                )

    def test_balancing_accounts_for_long_cases(self):
        rows = [
            (str(i), str(i), "recovery", v)
            for i, v in enumerate([300, 200, 100, 100, 100, 100])
        ]
        totals = shards.balance_records(rows)["estimated_seconds"]["recovery"]
        self.assertLessEqual(max(totals), 300)

    def test_duplicate_and_cross_family_groups_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            shards.balance_records(
                [("a", "g", "ordinary", 1), ("a", "h", "ordinary", 1)]
            )
        with self.assertRaisesRegex(ValueError, "crosses"):
            shards.balance_records(
                [("a", "g", "ordinary", 1), ("b", "g", "recovery", 1)]
            )

    def test_legacy_workflow_keeps_its_complete_two_lane_partition(self):
        entries = [item(f"test_a.py::test_{i}", i % 3 != 0) for i in range(120)]
        selected = []
        for shard in ("ordinary", "recovery-0", "recovery-1"):
            lane = entries.copy()
            shards.pytest_collection_modifyitems(config(shard), lane)
            selected.extend(i.nodeid for i in lane)
        self.assertEqual(sorted(selected), sorted(i.nodeid for i in entries))
        self.assertEqual(
            shards.shard_for_item(item("test_online_merge_recovery.py::test_codec")),
            "ordinary",
        )
        for shard in ("ordinary-0", "recovery-2"):
            with self.assertRaisesRegex(ValueError, "requires"):
                shards.pytest_collection_modifyitems(config(shard), entries)

    def test_identity_and_full_selection(self):
        names = (
            "test_a.py::test_a[x]",
            "e2e/antfly/test_a.py::test_a[x]",
            "e2e/antfly/test_a.py::test_a[x]@group",
        )
        self.assertEqual(len({shards.shard_for_item(item(n, True)) for n in names}), 1)
        entries = [item(n) for n in names]
        before = entries.copy()
        shards.pytest_collection_modifyitems(config("all"), entries)
        self.assertEqual(entries, before)

    def test_history_merge_preserves_other_lane_observations(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            baseline = root / "base.json"
            base = {
                "version": 1,
                "tests": {
                    "a": {"seconds": 1, "samples": 1},
                    "b": {"seconds": 2, "samples": 1},
                },
            }
            baseline.write_text(json.dumps(base))
            observations = []
            for node, seconds in [("a", 3), ("b", 4)]:
                data = json.loads(json.dumps(base))
                data["tests"][node] = {"seconds": seconds, "samples": 2}
                path = root / f"{node}.json"
                path.write_text(json.dumps(data))
                observations.append(path)
            result = merge(baseline, observations)
            self.assertEqual(result["tests"]["a"]["seconds"], 3)
            self.assertEqual(result["tests"]["b"]["seconds"], 4)
            with self.assertRaisesRegex(ValueError, "multiple lanes"):
                merge(baseline, [observations[0], observations[0]])

    def test_history_merge_migrates_prefixed_seed_and_measured_keys(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            baseline = root / "base.json"
            lane = root / "lane.json"
            baseline.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "tests": {
                            "e2e/antfly/test_a.py::test_case[a@b]": {
                                "seconds": 350,
                                "samples": 1,
                            },
                            "test_a.py::test_case[a@b]": {"seconds": 300, "samples": 2},
                        },
                    }
                )
            )
            lane.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "tests": {
                            "e2e/antfly/test_a.py::test_case[a@b]": {
                                "seconds": 350,
                                "samples": 1,
                            },
                            "test_a.py::test_case[a@b]": {"seconds": 240, "samples": 3},
                        },
                    }
                )
            )
            result = merge(baseline, [lane])
            self.assertEqual(
                result["tests"],
                {
                    "test_a.py::test_case[a@b]": {"seconds": 240, "samples": 3},
                },
            )
            self.assertEqual(
                shards.canonical_nodeid("e2e/antfly/test_a.py::test_case[a@b]@group"),
                "test_a.py::test_case[a@b]",
            )

    def test_summary_includes_setup_and_teardown_for_each_group(self):
        plan = shards.balance_records(
            [("a", "shared", "ordinary", 1), ("b", "shared", "ordinary", 1)]
        )
        text = summarize(
            plan,
            {
                "tests": {
                    "a": {"setup": 2, "call": 3, "teardown": 4},
                    "b": {"setup": 1, "call": 5, "teardown": 6},
                }
            },
        )
        self.assertIn("| `shared` | 3.00 | 8.00 | 10.00 |", text)
        self.assertIn("| teardown | 10.00 |", text)

    def test_workflow_requires_all_shards_plan_and_fd_lane(self):
        workflow = (
            Path(__file__).resolve().parents[2] / ".github/workflows/zig-tests.yml"
        ).read_text()
        base = workflow.split("  e2e-base-tests:\n", 1)[1].split(
            "  e2e-base-low-fd:\n", 1
        )[0]
        for family in ("ordinary", "recovery"):
            for i in range(3):
                self.assertIn(f"shard: {family}-{i}", base)
        self.assertNotIn("continue-on-error:", base)
        self.assertIn('ANTFLY_E2E_PROCESS_WORKERS: "1"', base)
        self.assertIn("ANTFLY_E2E_SHARD_PLAN=", base)
        self.assertIn("if: always()", base)
        self.assertNotIn("Run low-FD", base)
        low_fd = workflow.split("  e2e-base-low-fd:\n", 1)[1].split("  e2e-base:\n", 1)[
            0
        ]
        self.assertIn(
            "Retain failed low-FD server logs and cluster diagnostics", low_fd
        )
        self.assertIn("if: failure()", low_fd)
        self.assertIn("/native-stacks.txt", low_fd)
        self.assertIn("/failure-diagnostics.json", low_fd)
        self.assertIn("*.log", low_fd)
        gate = workflow.split("  e2e-base:\n")[1].split("  e2e-full-build:\n")[0]
        self.assertIn("e2e-base-plan, e2e-base-tests, e2e-base-low-fd]", gate)
        self.assertIn('test "$LOW_FD_RESULT" = "success"', gate)
        self.assertIn('test "$PLAN_RESULT" = "success"', gate)
        for block in workflow.split('if ! "$helper"')[1:3]:
            paths = shlex.split(
                block.split("\n          then", 1)[0]
                .split(" -- ", 1)[1]
                .replace("\\\n", " ")
            )
            for name in (
                "zig_e2e_shard.py",
                "merge_e2e_durations.py",
                "antfly_e2e_durations.json",
            ):
                self.assertIn("scripts/ci/" + name, paths)


if __name__ == "__main__":
    unittest.main()
