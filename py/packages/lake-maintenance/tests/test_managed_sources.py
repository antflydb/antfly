# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
import json
from types import SimpleNamespace

import boto3
import pytest
from botocore.stub import Stubber

from antfly_lake_maintenance.managed_sources import (
    AntflyTarget,
    ManagedSources,
    S3Notifications,
    exclusive_s3_writer,
)
from antfly_lake_maintenance.store import Conflict, Store


class Notifications:
    def __init__(self):
        self.created, self.deleted, self.acknowledged = False, False, False
        self.fail = True

    def provision(self, state, target):
        self.created = True
        if self.fail:
            self.fail = False
            raise TimeoutError("provider accepted create; response was lost")
        return {"owner": state["owner"]}

    def teardown(self, state, target):
        self.deleted = True

    def poll(self, state):
        return ["duplicated notification"]

    def ack(self, state, messages):
        self.acknowledged = True


def test_lost_provision_response_blocks_teardown_until_same_intent_recovers(tmp_path):
    provider = Notifications()
    manager = ManagedSources(Store(), tmp_path.as_uri() + "/", None, {"s3": provider})
    definition = {"kind": "s3", "table": "hn", "table_id": 7, "bucket": "data"}
    original = manager.define("archive", definition)
    with pytest.raises(TimeoutError):
        manager.reconcile("archive")
    assert provider.created and manager.status("archive")["phase"] == "provisioning"
    with pytest.raises(Conflict):
        manager.remove("archive")
    restored = ManagedSources(Store(), tmp_path.as_uri() + "/", None, {"s3": provider})
    active = restored.reconcile("archive")
    assert active["owner"] == original["owner"] and active["phase"] == "active"
    restored.remove("archive")
    assert restored.reconcile("archive")["phase"] == "removed"
    assert provider.deleted
    with pytest.raises(Conflict):
        restored.define("archive", {**definition, "table_id": 8})


def test_notifications_are_not_acknowledged_before_durable_native_handoff(tmp_path):
    provider = Notifications()
    provider.fail = False
    target = SimpleNamespace(
        reconcile_lake=lambda definition: "s3://data/metadata/current.json"
    )
    manager = ManagedSources(Store(), tmp_path.as_uri() + "/", target, {"s3": provider})
    manager.define("archive", {"kind": "s3", "table": "hn", "table_id": 7})
    manager.reconcile("archive")
    original_save = manager._save

    def failed_save(*args):
        raise TimeoutError("authority failed")

    manager._save = failed_save
    with pytest.raises(TimeoutError):
        manager.poll("archive")
    assert not provider.acknowledged
    manager._save = original_save
    manager.poll("archive")
    assert provider.acknowledged
    assert manager.status("archive")["checkpoint"] == "s3://data/metadata/current.json"


def test_native_cdc_configuration_preserves_other_sources_and_fences_recreate():
    target = AntflyTarget("http://unused")
    definition = {
        "table": "hn",
        "table_id": 7,
        "dsn_ref": "${secret:PG}",
        "postgres_table": "public.hn",
    }
    names = {"slot": "owned", "publication": "owned_pub"}
    calls = []
    other = {"slot_name": "operator_owned"}
    target.configuration = lambda table: {
        "table_id": 7,
        "sources_hash": "old",
        "replication_sources": [other],
    }
    target.request = lambda *args: calls.append(args) or {}
    target.configure_postgres(definition, names)
    assert calls[0][3]["replication_sources"][0] == other
    assert calls[0][3]["expected_sources_hash"] == "old"
    target.configuration = lambda table: {"table_id": 8}
    with pytest.raises(Conflict):
        target.configure_postgres(definition, names, remove=True)


def test_s3_requires_an_enforced_exclusive_configuration_writer():
    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="fixture",
        aws_secret_access_key="fixture",
    )
    role = "arn:aws:iam::123456789012:role/antfly"
    sts = SimpleNamespace(
        get_caller_identity=lambda: {
            "Account": "123456789012",
            "Arn": "arn:aws:sts::123456789012:assumed-role/antfly/test",
        }
    )
    verify = exclusive_s3_writer(s3, sts, role)
    with Stubber(s3) as stub:
        stub.add_response(
            "get_bucket_policy",
            {"Policy": json.dumps({"Statement": []})},
            {"Bucket": "data"},
        )
        with pytest.raises(PermissionError):
            verify("data")
        policy = {
            "Statement": [
                {
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:PutBucketNotification",
                    "Resource": "arn:aws:s3:::data",
                    "Condition": {"ArnNotEquals": {"aws:PrincipalArn": role}},
                }
            ]
        }
        stub.add_response(
            "get_bucket_policy", {"Policy": json.dumps(policy)}, {"Bucket": "data"}
        )
        verify("data")


def test_s3_notification_provision_preserves_unrelated_configuration():
    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        aws_access_key_id="fixture",
        aws_secret_access_key="fixture",
    )
    sqs = boto3.client(
        "sqs",
        region_name="us-east-1",
        aws_access_key_id="fixture",
        aws_secret_access_key="fixture",
    )
    owner = "a" * 64
    state = {
        "owner": owner,
        "definition": {"bucket": "data", "prefix": "hn/", "account_id": "123456789012"},
    }
    identifier, url = (
        "antfly-" + owner[:40],
        "https://sqs.us-east-1.amazonaws.com/123456789012/owned",
    )
    arn = "arn:aws:sqs:us-east-1:123456789012:owned"
    operator = {
        "Id": "operator",
        "TopicArn": "arn:aws:sns:us-east-1:123456789012:other",
        "Events": ["s3:ObjectCreated:*"],
    }
    wanted = {
        "Id": identifier,
        "QueueArn": arn,
        "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
        "Filter": {"Key": {"FilterRules": [{"Name": "prefix", "Value": "hn/"}]}},
    }
    with Stubber(s3) as buckets, Stubber(sqs) as queues:
        queues.add_response(
            "get_queue_url", {"QueueUrl": url}, {"QueueName": identifier}
        )
        queues.add_response(
            "list_queue_tags", {"Tags": {"antfly-owner": owner}}, {"QueueUrl": url}
        )
        queues.add_response(
            "get_queue_attributes",
            {"Attributes": {"QueueArn": arn}},
            {"QueueUrl": url, "AttributeNames": ["QueueArn"]},
        )
        queues.add_response("set_queue_attributes", {}, expected_params=None)
        buckets.add_response(
            "get_bucket_notification_configuration",
            {"TopicConfigurations": [operator]},
            {"Bucket": "data"},
        )
        buckets.add_response(
            "put_bucket_notification_configuration",
            {},
            {
                "Bucket": "data",
                "NotificationConfiguration": {
                    "TopicConfigurations": [operator],
                    "QueueConfigurations": [wanted],
                },
            },
        )
        resources = S3Notifications(
            s3, sqs, verify_exclusive_configuration_writer=lambda bucket: None
        ).provision(state, None)
        assert resources["queue_url"] == url


def test_supervisor_resumes_unknown_intent_and_reports_redacted_failure(tmp_path):
    provider = Notifications()
    target = SimpleNamespace(reconcile_lake=lambda definition: "s3://data/current.json")
    manager = ManagedSources(Store(), tmp_path.as_uri() + "/", target, {"s3": provider})
    manager.define("archive", {"kind": "s3", "table": "hn", "table_id": 7})
    with pytest.raises(TimeoutError):
        manager.step("archive")
    status = manager.status("archive")
    assert status["last_attempt"]["error_type"] == "TimeoutError"
    assert "response was lost" not in json.dumps(status)
    restarted = ManagedSources(
        Store(), tmp_path.as_uri() + "/", target, {"s3": provider}
    )
    assert restarted.step("archive")["phase"] == "active"
    assert "last_attempt" not in restarted.status("archive")
    assert restarted.step("archive")["checkpoint"] == "s3://data/current.json"
    assert provider.acknowledged
    restarted.remove("archive")
    assert restarted.step("archive")["phase"] == "removed"
