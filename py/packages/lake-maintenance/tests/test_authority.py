# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from antfly_lake_maintenance.controller import Controller
from antfly_lake_maintenance.server import resolve
from antfly_lake_maintenance.store import Conflict, Store, Unavailable, digest, encode


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


@pytest.mark.parametrize("protected_expiry", [100_000_000_000, 70_000_000_000])
def test_committed_writer_reclaims_expired_readers_and_preserves_grace(
    tmp_path, protected_expiry
):
    controller = object.__new__(Controller)
    controller.config = {"authority_uri": tmp_path.as_uri() + "/", "max_readers": 2}
    controller.store = Store()
    controller.now_ns = lambda: 100_000_000_000
    protected = {"id": "protected", "expires_ns": protected_expiry}
    controller.store.mutate(
        controller.key("HEAD.json"),
        lambda state: state.update(
            writer={"operation": "committed", "phase": "sent"},
            readers={"expired": {"expires_ns": 1}, "protected": protected},
        ),
    )
    lease = {"id": "new", "expires_ns": 220_000_000_000}
    controller._finish_writer("committed", lease)
    state = controller.state()
    assert state["writer"] is None
    assert state["readers"] == {"protected": protected, "new": lease}
    # Reclaiming expired readers must not bypass the active-reader limit.
    controller.store.mutate(
        controller.key("HEAD.json"),
        lambda state: state.update(writer={"operation": "next", "phase": "sent"}),
    )
    with pytest.raises(Unavailable, match="commit reader admission budget exceeded"):
        controller._finish_writer(
            "next", {"id": "extra", "expires_ns": 220_000_000_000}
        )
    assert controller.state()["readers"] == state["readers"]
    assert controller.state()["writer"]["operation"] == "next"


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


def test_native_pin_cleanup_progresses_past_expired_inventory_limit(tmp_path):
    from antfly_lake_maintenance.planning import PlanningPending
    from antfly_lake_maintenance.store import Unavailable

    controller = object.__new__(Controller)
    controller.config = {"max_readers": 2}
    controller.store = Store()
    controller.now_ns = lambda: 1_000_000_000_000
    registry = tmp_path.as_uri() + "/snapshots/"
    controller.store.put(registry + "pins/000-active", b"2000000000000", absent=True)
    for index in range(5):
        controller.store.put(registry + f"pins/100-expired-{index}", b"1", absent=True)
    for _ in range(2):
        with pytest.raises(PlanningPending):
            controller._native_pins(registry)
    assert controller._native_pins(registry) == {"000-active"}
    assert len(list(controller.store.inventory(registry + "pins/"))) == 1
    for index in range(2):
        controller.store.put(
            registry + f"pins/200-active-{index}", b"2000000000000", absent=True
        )
    with pytest.raises(Unavailable, match="native reader inventory budget"):
        controller._native_pins(registry)


def test_native_pin_cleanup_preserves_concurrent_renewal(tmp_path):
    from antfly_lake_maintenance.planning import PlanningPending

    class RenewingStore(Store):
        def delete_current(self, uri, version):
            self.put(uri, b"2000000000000", version=version)
            super().delete_current(uri, version)

    controller = object.__new__(Controller)
    controller.config = {"max_readers": 2}
    controller.store = RenewingStore()
    controller.now_ns = lambda: 1_000_000_000_000
    registry = tmp_path.as_uri() + "/snapshots/"
    controller.store.put(registry + "pins/renewed", b"1", absent=True)
    with pytest.raises(PlanningPending):
        controller._native_pins(registry)
    assert controller._native_pins(registry) == {"renewed"}


def test_native_pin_cleanup_rereads_after_exact_generation_deletion(tmp_path):
    class RenewingStore(Store):
        def delete_current(self, uri, version):
            # GCS may delete the specified old generation while retaining a
            # concurrently renewed current generation.
            self.put(uri, b"2000000000000", version=version)

    controller = object.__new__(Controller)
    controller.config = {"max_readers": 2}
    controller.store = RenewingStore()
    controller.now_ns = lambda: 1_000_000_000_000
    registry = tmp_path.as_uri() + "/snapshots/"
    controller.store.put(registry + "pins/renewed", b"1", absent=True)
    assert controller._native_pins(registry) == {"renewed"}


def test_s3_mutable_pin_deletion_uses_current_etag_condition():
    from botocore.exceptions import ClientError

    class S3:
        def delete_object(self, **kwargs):
            assert kwargs == {
                "Bucket": "archive",
                "Key": "pins/expired",
                "IfMatch": '"old"',
            }
            raise ClientError(
                {
                    "Error": {"Code": "PreconditionFailed"},
                    "ResponseMetadata": {"HTTPStatusCode": 412},
                },
                "DeleteObject",
            )

    store = Store()
    store._s3 = S3()
    with pytest.raises(Conflict, match="delete object changed"):
        store.delete_current("s3://archive/pins/expired", '"old"')
