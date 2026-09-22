# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Elastic-2.0

"""Read-only startup barrier for tests that publish V17 store reports."""

import time

import requests

# Dense-native storage capability requires the named framed V17 profile.
DENSE_NATIVE_STATUS_PROFILE = 17


def wait_for_catalog_protocol(cluster, *, timeout_s=15.0):
    deadline = time.monotonic() + timeout_s
    observations = {}
    while time.monotonic() < deadline:
        for index, process in enumerate(cluster.metadata_procs):
            assert process.poll() is None, (
                f"metadata node {index} exited before protocol readiness: "
                f"{process.returncode}\n{cluster.debug_logs()}"
            )
        for index, url in enumerate(cluster.metadata_urls):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                response = requests.get(
                    url + "/metadata/v1/status", timeout=min(5.0, remaining)
                )
                if response.status_code == 503:
                    observations[index] = (503, response.text)
                    continue
                response.raise_for_status()
                status = response.json()
            except (requests.ConnectionError, requests.Timeout) as exc:
                observations[index] = repr(exc)
                continue
            observations[index] = status
            # Registration visibility and HTTP readiness do not establish
            # that the leader can persist the reporter's required profile.
            # A ready version is sufficient: registration itself can commit
            # activation. Waiting only for durable activation would deadlock.
            if (
                status.get("metadata_raft_role") == "leader"
                and status.get("metadata_incarnation")
                and status.get("runtime_status_protocol_ready_version")
                == DENSE_NATIVE_STATUS_PROFILE
            ):
                return index
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(0.1, remaining))
    raise AssertionError(
        f"runtime-status profile {DENSE_NATIVE_STATUS_PROFILE} was not ready "
        f"within {timeout_s}s: {observations}\n{cluster.debug_logs()}"
    )
