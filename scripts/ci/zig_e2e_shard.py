# Copyright 2026 Antfly, Inc.
# Licensed under the Elastic License 2.0 (ELv2).

"""Exhaustive CI lanes for ordinary tests and real distributed recovery.

Use pytest's transitive fixture closure, not a list of test names: new cases
using the six-process cluster automatically enter one required recovery lane.
This plugin is opt-in; local/default and full-suite runs retain all tests.
"""

import zlib

SHARDS = ("all", "ordinary", "recovery-0", "recovery-1")
DISTRIBUTED_FIXTURE = "three_by_three_backup_cluster"


def canonical_nodeid(nodeid: str) -> str:
    # pytest-xdist appends @isolation-group after collection. Membership must
    # not depend on hook order or on running pytest from the repo/zig/test root.
    path, separator, test = nodeid.split("@", 1)[0].partition("::")
    return path.replace("\\", "/").rsplit("/", 1)[-1] + separator + test


def shard_for_item(item) -> str:
    if DISTRIBUTED_FIXTURE not in item.fixturenames:
        return "ordinary"
    bucket = zlib.crc32(canonical_nodeid(item.nodeid).encode("utf-8")) % 2
    return f"recovery-{bucket}"


def pytest_addoption(parser):
    parser.getgroup("antfly-ci").addoption(
        "--antfly-ci-shard",
        choices=SHARDS,
        default="all",
        help="Required Antfly CI lane (default: run the complete selection)",
    )


def pytest_collection_modifyitems(config, items):
    shard = config.getoption("antfly_ci_shard")
    if shard == "all":
        return
    selected, deselected = [], []
    for item in items:
        (selected if shard_for_item(item) == shard else deselected).append(item)
    items[:] = selected
    if deselected:
        config.hook.pytest_deselected(items=deselected)
