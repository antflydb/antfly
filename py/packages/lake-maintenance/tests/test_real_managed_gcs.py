# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Opt-in cloud provisioning, delivery, ownership reconciliation and teardown."""

import json
import os
import subprocess
import time
import uuid
from types import SimpleNamespace

import pytest

from antfly_lake_maintenance.managed_sources import GCSNotifications, ManagedSources
from antfly_lake_maintenance.store import Store


@pytest.mark.skipif(
    not os.environ.get("ANTFLY_MANAGED_GCS_PROJECT"),
    reason="set an explicitly authorized disposable GCS qualification project",
)
def test_real_gcs_notification_lifecycle(tmp_path):
    from google.api_core.exceptions import NotFound
    from google.cloud import pubsub_v1, storage
    from google.oauth2.credentials import Credentials

    project = os.environ["ANTFLY_MANAGED_GCS_PROJECT"]
    token = subprocess.run(
        ["gcloud", "auth", "print-access-token"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    credentials = Credentials(token)
    client = storage.Client(project=project, credentials=credentials)
    publisher = pubsub_v1.PublisherClient(credentials=credentials)
    subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
    name = project + "-antfly-managed-qual-" + uuid.uuid4().hex[:12]
    bucket = client.create_bucket(name, location="US-WEST1")
    provider = GCSNotifications(client, publisher, subscriber)
    # Isolate provider qualification from the already separately qualified
    # native catalog handoff. No fake Pub/Sub or Storage implementation.
    handoffs = []
    target = SimpleNamespace(
        reconcile_lake=lambda definition: (
            handoffs.append(definition) or "gcs://" + name + "/data/"
        )
    )
    manager = ManagedSources(
        Store(), tmp_path.as_uri() + "/", target, {"gcs": provider}
    )
    state = manager.define(
        "archive",
        {
            "kind": "gcs",
            "project": project,
            "bucket": name,
            "prefix": "data/",
            "table": "hn",
            "table_id": 7,
        },
    )
    topic, subscription = provider._paths(state)
    original_poll = provider.poll
    received = []

    def observed_poll(value):
        messages = original_poll(value)
        received.extend(messages)
        return messages

    provider.poll = observed_poll
    try:
        manager.reconcile("archive")
        manager.reconcile("archive")
        assert len(list(bucket.list_notifications())) == 1
        assert (
            subscriber.get_subscription(request={"subscription": subscription}).topic
            == topic
        )
        bucket.blob("data/qualification.json").upload_from_string(json.dumps({"id": 7}))
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline and not received:
            manager.poll("archive")
            time.sleep(0.2)
        assert received, "real GCS notification did not reach the owned subscription"
        assert (
            handoffs
            and manager.status("archive")["checkpoint"] == "gcs://" + name + "/data/"
        )
        manager.remove("archive")
        assert manager.reconcile("archive")["phase"] == "removed"
        assert list(bucket.list_notifications()) == []
        for client_, method, argument, resource in (
            (publisher, "get_topic", "topic", topic),
            (subscriber, "get_subscription", "subscription", subscription),
        ):
            with pytest.raises(NotFound):
                getattr(client_, method)(request={argument: resource})
    finally:
        # Deterministic names and ownership witnesses also clean up resources
        # created by an attempt whose final journal write did not return.
        try:
            provider.teardown(state, target)
        finally:
            bucket.delete(force=True)
            publisher.transport.close()
            subscriber.transport.close()
