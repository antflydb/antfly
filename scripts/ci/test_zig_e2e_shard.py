import shlex
import unittest
from pathlib import Path
from types import SimpleNamespace

import zig_e2e_shard as shards


def item(name, *, distributed=False):
    return SimpleNamespace(
        nodeid=name,
        fixturenames=["request", "wrapper", shards.DISTRIBUTED_FIXTURE]
        if distributed
        else ["request", "backup_api"],
    )


class ShardTests(unittest.TestCase):
    def test_partition_is_exhaustive_disjoint_and_order_independent(self):
        items = [
            item(
                f"e2e/antfly/test_future.py::test_new_case[{n}]", distributed=n % 3 != 0
            )
            for n in range(120)
        ]
        membership = {entry.nodeid: shards.shard_for_item(entry) for entry in items}
        self.assertEqual(set(membership.values()), set(shards.SHARDS) - {"all"})
        selected = {}
        for shard in shards.SHARDS[1:]:
            collected = list(reversed(items))
            deselected = []
            config = SimpleNamespace(
                getoption=lambda name, shard=shard: shard,
                hook=SimpleNamespace(
                    pytest_deselected=lambda *, items, deselected=deselected: (
                        deselected.extend(items)
                    )
                ),
            )
            shards.pytest_collection_modifyitems(config, collected)
            selected[shard] = {entry.nodeid for entry in collected}
            self.assertEqual(len(collected) + len(deselected), len(items))
            self.assertTrue(
                all(membership[entry.nodeid] == shard for entry in collected)
            )
        self.assertEqual(set.union(*selected.values()), set(membership))
        self.assertEqual(sum(map(len, selected.values())), len(items))

    def test_new_nested_fixture_users_automatically_enter_recovery(self):
        self.assertTrue(
            shards.shard_for_item(
                item("test_future.py::test_new", distributed=True)
            ).startswith("recovery-")
        )
        # A unit test colocated with recovery tests must not start consuming a
        # recovery lane merely because of its filename.
        self.assertEqual(
            shards.shard_for_item(item("test_online_merge_recovery.py::test_codec")),
            "ordinary",
        )

    def test_collection_root_and_xdist_suffix_do_not_change_membership(self):
        names = (
            "test_new.py::test_case[snapshot-owner]",
            "e2e/antfly/test_new.py::test_case[snapshot-owner]",
            "e2e/antfly/test_new.py::test_case[snapshot-owner]@antfly-process--test--123",
        )
        self.assertEqual(
            len(
                {shards.shard_for_item(item(name, distributed=True)) for name in names}
            ),
            1,
        )

    def test_all_is_an_unmodified_full_selection(self):
        entries = [
            item("test_a.py::test_a"),
            item("test_b.py::test_b", distributed=True),
        ]
        original = entries.copy()
        config = SimpleNamespace(getoption=lambda name: "all")
        shards.pytest_collection_modifyitems(config, entries)
        self.assertEqual(entries, original)

    def test_workflow_requires_every_lane_and_uses_diskful_local_scratch(self):
        root = Path(__file__).resolve().parents[2]
        workflow = (root / ".github/workflows/zig-tests.yml").read_text()
        base = workflow.split("  e2e-base-tests:\n", 1)[1].split("\n  e2e-base:\n", 1)[
            0
        ]
        for shard in shards.SHARDS[1:]:
            self.assertIn(f"shard: {shard}", base)
        self.assertIn("ANTFLY_E2E_SHARD: ${{ matrix.shard }}", base)
        self.assertIn('ANTFLY_E2E_PROCESS_SLOTS: "2"', base)
        self.assertIn('ANTFLY_E2E_PROCESS_WORKERS: "1"', base)
        self.assertNotIn("continue-on-error:", base)
        self.assertIn(
            "needs: [admission, changes, e2e-base-build, e2e-base-tests]", workflow
        )
        self.assertEqual(workflow.count('test_tmp="${RUNNER_TEMP}/antfly-e2e/'), 2)
        self.assertNotIn('test_tmp="/mnt/cache/', workflow)
        self.assertNotIn('test_tmp="/dev/shm/', workflow)

    def test_sharding_changes_select_both_build_and_e2e_validation(self):
        root = Path(__file__).resolve().parents[2]
        workflow = (root / ".github/workflows/zig-tests.yml").read_text()
        # The first two filters select Zig validation and E2E respectively.
        # Python shard-only changes must not disappear behind the shell glob.
        for block in workflow.split('if ! "$helper"')[1:3]:
            command = block.split("\n          then", 1)[0]
            pathspecs = shlex.split(command.split(" -- ", 1)[1].replace("\\\n", " "))
            self.assertIn("scripts/ci/zig_e2e_shard.py", pathspecs)
            self.assertIn("scripts/ci/test_zig_e2e_shard.py", pathspecs)

    def test_prs_without_required_sharding_fail_closed(self):
        root = Path(__file__).resolve().parents[2]
        workflow = (root / ".github/workflows/zig-tests.yml").read_text()
        self.assertIn(
            '"$ANTFLY_E2E_SUITE" == antfly* && ! -f scripts/ci/zig_e2e_shard.py',
            workflow,
        )
        guard = workflow.split('if [[ "$ANTFLY_E2E_SUITE" == antfly*', 1)[1].split(
            "fi", 1
        )[0]
        self.assertIn("Merge origin/main", guard)
        self.assertIn("exit 1", guard)
        self.assertNotIn("exit 0", guard)


if __name__ == "__main__":
    unittest.main()
