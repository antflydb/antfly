# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from antfly_lake_maintenance.controller import Controller
from antfly_lake_maintenance.server import resolve
from antfly_lake_maintenance.store import Conflict, Store, digest, encode


def test_authority_concurrent_owners_do_not_lose_transitions(tmp_path):
    store = Store()
    uri = tmp_path.as_uri() + "/HEAD.json"

    def increment(_):
        try:
            store.mutate(
                uri, lambda value: value.update(count=value.get("count", 0) + 1)
            )
            return True
        except Conflict:
            # Contention is bounded: rejected calls must not lose or duplicate
            # an acknowledged transition.
            return False

    with ThreadPoolExecutor(max_workers=4) as workers:
        accepted = sum(workers.map(increment, range(40)))
    state = json.loads(store.get(uri)[0])
    assert accepted > 0
    assert state["count"] == state["sequence"] == accepted
    # ABA is impossible even when all mutable admission fields return to empty.
    old = store.get(uri)[1]
    store.mutate(uri, lambda value: None)
    with pytest.raises(Conflict):
        store.put(uri, encode(state), version=old)


def test_original_plan_cannot_be_replaced_after_restart(tmp_path):
    uri = tmp_path.as_uri() + "/plan.json"
    Store().immutable(uri, b"original")
    restarted = Store()
    restarted.immutable(uri, b"original")
    with pytest.raises(Conflict):
        restarted.immutable(uri, b"replacement")
    assert restarted.get(uri)[0] == b"original"


def test_exact_filesystem_deletion_preserves_replacement(tmp_path):
    store = Store()
    uri = tmp_path.as_uri() + "/data.parquet"
    store.put(uri, b"old", absent=True)
    old = store.get(uri)[1]
    store.put(uri, b"new", version=old)
    with pytest.raises(Conflict):
        store.delete(uri, old)
    assert store.get(uri)[0] == b"new"


def test_unsent_writer_recovery_cannot_release_sent_request(tmp_path):
    controller = object.__new__(Controller)
    controller.config = {"authority_uri": tmp_path.as_uri() + "/"}
    controller.store = Store()
    uri = controller.key("HEAD.json")
    controller.store.mutate(
        uri,
        lambda value: value.update(writer={"operation": "write", "phase": "admitted"}),
    )
    original_state = controller.state

    def race():
        observed = original_state()
        controller.store.mutate(uri, lambda value: value["writer"].update(phase="sent"))
        return observed

    controller.state = race
    with pytest.raises(Conflict):
        controller.recover_writer()
    assert original_state()["writer"]["phase"] == "sent"


def test_failed_planner_cannot_release_prepared_job(tmp_path):
    controller = object.__new__(Controller)
    controller.config = {"authority_uri": tmp_path.as_uri() + "/"}
    controller.store = Store()
    controller.store.mutate(
        controller.key("HEAD.json"),
        lambda value: value.update(vacuum={"id": "job", "phase": "prepared"}),
    )
    with pytest.raises(Conflict):
        controller._release_job("job", planning_only=True)
    assert controller.state()["vacuum"] == {"id": "job", "phase": "prepared"}


def test_secrets_require_explicit_environment_binding(monkeypatch):
    monkeypatch.setenv("TOKEN_TEST", "secret")
    assert resolve({"token": {"env": "TOKEN_TEST"}}) == {"token": "secret"}
    with pytest.raises(KeyError):
        resolve({"env": "ABSENT_ANTFLY_SECRET"})


def test_artifact_writers_cannot_share_controller_authority(tmp_path):
    config = {
        "provider": "polaris",
        "warehouse_uri": tmp_path.as_uri() + "/warehouse/",
        "authority_uri": tmp_path.as_uri() + "/coordination/",
        "artifact_uri": tmp_path.as_uri() + "/coordination/artifacts/",
        "gateway_enforced": True,
    }
    with pytest.raises(ValueError, match="disjoint"):
        Controller(config, Store())


@pytest.mark.parametrize("bad_retention", [float("nan"), float("inf"), True, "600000"])
def test_retention_cannot_bypass_minimum_age_with_nonintegers(bad_retention):
    from antfly_lake_maintenance.store import digest

    controller = object.__new__(Controller)
    controller.config = {
        "provider": "polaris",
        "catalog_uri": "https://gateway/catalog",
    }
    body = encode(
        {
            "protocol": 1,
            "provider": "polaris",
            "catalog": {"uri": "https://gateway/catalog"},
            "policy": {
                "retain_ms": bad_retention,
                "keep_latest": 1,
                "max_deleted": 1,
                "dry_run": False,
            },
        }
    )
    with pytest.raises(ValueError, match="integers"):
        controller.run_job(digest(body), body)


@pytest.mark.parametrize("artifact_prefix", ["", "journal/", "nested/journal/"])
def test_job_registry_matches_native_root_and_nested_prefixes(artifact_prefix):
    controller = object.__new__(Controller)
    controller.config = {
        "artifact_uri": "s3://artifacts/" + artifact_prefix,
        "artifact_connection": "s3",
    }
    job = {
        "source_uri": "s3://warehouse/table",
        "table_uuid": "table-uuid",
    }
    prefix = (
        artifact_prefix
        + "lake-readers/"
        + digest(encode([job["source_uri"], job["table_uuid"]]))
    )
    job["reader_registry"] = {
        "protocol": "antfly-snapshot-pins-v1",
        "connection": "s3",
        "bucket": "artifacts",
        "prefix": prefix,
        "lease_grace_ms": 30_000,
    }
    assert controller._job_registry(job) == f"s3://artifacts/{prefix}/snapshots/"
    job["reader_registry"]["prefix"] = "/" + prefix
    with pytest.raises(ValueError, match="does not match configured authority"):
        controller._job_registry(job)
