# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Restartable managed resources with persisted intent and exact ownership.

A durable authority serializes operation intents, including all sources
sharing a bucket. No provider action silently takes ownership of an existing
resource. Credentials are supplied at runtime, never stored in this journal.
"""

import contextlib
import json
import time
from urllib.parse import quote
from urllib.request import Request, build_opener

from .provider import NoRedirect

from .store import Conflict, Unavailable, digest, encode


class AntflyTarget:
    def __init__(self, endpoint, headers=None):
        self.endpoint, self.headers = endpoint.rstrip("/"), headers or {}

    def request(self, method, table, suffix, body=None):
        path = self.endpoint + "/tables/" + quote(table, safe="") + suffix
        request = Request(
            path,
            None if body is None else encode(body),
            {**self.headers, "Content-Type": "application/json"},
            method=method,
        )
        with build_opener(NoRedirect).open(request, timeout=30) as response:
            raw = response.read(4 * 1024 * 1024 + 1)
            if len(raw) > 4 * 1024 * 1024:
                raise Unavailable("native source response exceeds budget")
            return json.loads(raw)

    def configuration(self, table):
        return self.request("GET", table, "/sources/managed")

    def configure_postgres(self, definition, names, remove=False):
        current = self.configuration(definition["table"])
        if current["table_id"] != definition["table_id"]:
            raise Conflict("destination table was recreated")
        sources = current["replication_sources"]
        matching = [
            source for source in sources if source.get("slot_name") == names["slot"]
        ]
        expected = {
            "type": "postgres",
            "dsn": definition["dsn_ref"],
            "postgres_table": definition["postgres_table"],
            "slot_name": names["slot"],
            "publication_name": names["publication"],
            "key_template": definition.get("key_template", "id"),
            "require_exact_cutover": True,
            "on_delete": [{"op": "$delete_document"}],
        }
        # Sealed native authorization fields may be present; public source
        # properties still have to match before replacing/removing this entry.
        if any(
            any(source.get(key) != value for key, value in expected.items())
            for source in matching
        ):
            raise Conflict("managed CDC entry was modified outside its owner")
        if remove and not matching:
            return current
        if not remove and matching:
            return current
        replacement = [
            source for source in sources if source.get("slot_name") != names["slot"]
        ]
        if not remove:
            replacement.append(expected)
        return self.request(
            "POST",
            definition["table"],
            "/sources/managed",
            {
                "table_id": definition["table_id"],
                "expected_sources_hash": current["sources_hash"],
                "replication_sources": replacement,
            },
        )

    def reconcile_lake(self, definition):
        result = self.request(
            "POST",
            definition["table"],
            "/lake/reconcile",
            {"table_id": definition["table_id"]},
        )
        # A wakeup is repairable because the committed catalog is the durable
        # authority. It is not a claim that indexing has finished.
        if result["state"] != "reconciled":
            raise Unavailable("native source reconciliation not accepted")
        return result["metadata_location"]


class ManagedSources:
    def __init__(self, store, authority_uri, target, providers, *, now=time.time_ns):
        if not authority_uri.endswith("/"):
            raise ValueError("source authority must end in slash")
        self.store, self.authority, self.target = store, authority_uri, target
        self.providers, self.now = providers, now

    def _uri(self, identifier):
        if (
            not identifier
            or len(identifier) > 128
            or not all(c.isalnum() or c in "-_" for c in identifier)
        ):
            raise ValueError("invalid source identifier")
        return self.authority + "sources/" + identifier + ".json"

    def define(self, identifier, definition):
        if definition["kind"] not in self.providers or definition["table_id"] <= 0:
            raise ValueError("invalid managed source")
        if definition.get("mode", "managed") != "managed":
            raise ValueError("managed setup cannot adopt operator-owned resources")
        if definition["kind"] == "postgres" and not definition.get(
            "dsn_ref", ""
        ).startswith("${secret:"):
            raise ValueError("PostgreSQL DSN must be a native secret reference")
        # Definition deliberately contains credential references only.
        if any(
            key in definition for key in ("password", "dsn", "token", "credentials")
        ):
            raise ValueError("inline credentials cannot be persisted")
        fingerprint = digest(encode(definition))
        owner = digest(encode([self.authority, identifier, fingerprint]))
        state = {
            "definition": definition,
            "fingerprint": fingerprint,
            "owner": owner,
            "phase": "provisioning",
            "resources": {},
            "checkpoint": None,
            "revision": 0,
        }
        uri = self._uri(identifier)
        existing = self.store.get(uri)
        if existing:
            value = json.loads(existing[0])
            if value["fingerprint"] != fingerprint:
                raise Conflict(
                    "source definition requires an explicit replacement lifecycle"
                )
            return value
        self.store.put(uri, encode(state), absent=True)
        return state

    def status(self, identifier):
        saved = self.store.get(self._uri(identifier))
        if saved is None:
            raise KeyError(identifier)
        value = json.loads(saved[0])
        diagnostic = self.store.get(
            self.authority + "source-status/" + identifier + ".json"
        )
        if diagnostic:
            observation = json.loads(diagnostic[0])
            if observation["revision"] == value["revision"]:
                value["last_attempt"] = observation
        return value

    def _save(self, identifier, original, replacement):
        uri = self._uri(identifier)
        saved = self.store.get(uri)
        if saved is None or json.loads(saved[0])["revision"] != original["revision"]:
            raise Conflict("managed source reconciliation ownership changed")
        replacement.pop("last_attempt", None)
        replacement["revision"] = original["revision"] + 1
        self.store.put(uri, encode(replacement), version=saved[1])
        return replacement

    @contextlib.contextmanager
    def _operation(self, identifier, state, action):
        uri = self.authority + "source-owner.json"
        intent = {"source": identifier, "revision": state["revision"], "action": action}

        def admit(authority):
            previous = authority.get("operation")
            if previous and previous != intent:
                committed = self.status(previous["source"])
                if committed["revision"] <= previous["revision"]:
                    raise Conflict("another source operation has an unresolved outcome")
            authority["operation"] = intent

        self.store.mutate(uri, admit)
        # Never release on failure: an SDK timeout can mean the provider has
        # accepted the request. Resume this same intent using ownership proofs.
        try:
            yield
        except Exception as error:
            error_type = type(error).__name__
            # Observational status never changes operation ownership/revision.
            # Store the class only: provider messages can contain credentials.
            try:
                self.store.mutate(
                    self.authority + "source-status/" + identifier + ".json",
                    lambda record: record.update(
                        {
                            "revision": state["revision"],
                            "action": action,
                            "at_ns": self.now(),
                            "error_type": error_type,
                        }
                    ),
                )
            except Exception:
                pass
            raise

        def release(authority):
            if authority.get("operation") != intent:
                raise Conflict("source operation ownership changed")
            authority["operation"] = None

        self.store.mutate(uri, release)

    def remove(self, identifier):
        state = self.status(identifier)
        if state["phase"] == "removed":
            return state
        with self._operation(identifier, state, "remove"):
            return self._save(identifier, state, {**state, "phase": "teardown"})

    def reconcile(self, identifier):
        original = self.status(identifier)
        state = {**original}
        provider = self.providers[state["definition"]["kind"]]
        if state["phase"] == "removed":
            return state
        with self._operation(identifier, original, "reconcile"):
            if state["phase"] == "teardown":
                provider.teardown(state, self.target)
                state["phase"] = "removed"
            else:
                state["resources"] = provider.provision(state, self.target)
                state["phase"] = "active"
            state["reconciled_ns"] = self.now()
            return self._save(identifier, original, state)

    def poll(self, identifier):
        original = self.status(identifier)
        if original["phase"] != "active":
            raise Conflict("source is not active")
        provider = self.providers[original["definition"]["kind"]]
        with self._operation(identifier, original, "poll"):
            if original["definition"]["kind"] == "postgres":
                observation = provider.observe(original, self.target)
                return self._save(
                    identifier,
                    original,
                    {
                        **original,
                        "provider_status": observation,
                        "last_poll_ns": self.now(),
                    },
                )
            messages = provider.poll(original)
            # Periodic reconciliation is required even with an empty queue;
            # cloud events may be duplicated, reordered or lost.
            checkpoint = self.target.reconcile_lake(original["definition"])
            state = self._save(
                identifier,
                original,
                {**original, "checkpoint": checkpoint, "last_poll_ns": self.now()},
            )
            provider.ack(original, messages)
            return state

    def step(self, identifier):
        """One bounded supervisor turn, resuming an unresolved provider intent."""
        state = self.status(identifier)
        authority = self.store.get(self.authority + "source-owner.json")
        operation = json.loads(authority[0]).get("operation") if authority else None
        if (
            operation
            and operation["source"] == identifier
            and operation["revision"] == state["revision"]
        ):
            # Resume exactly the action whose outcome was unknown. Reconcile
            # cannot supersede a timed-out poll or vice versa.
            return getattr(self, operation["action"])(identifier)
        if state["phase"] == "removed":
            return state
        if state["phase"] != "active":
            return self.reconcile(identifier)
        # Revalidate ownership/configuration periodically before polling.
        self.reconcile(identifier)
        return self.poll(identifier)


def owned_name(state, prefix):
    return prefix + state["owner"][:40]


class PostgreSQLCDC:
    def __init__(self, connect):
        # connect(secret_reference) resolves a runtime credential and returns
        # a psycopg connection. Native workers use the same secret reference.
        self.connect = connect

    def _names(self, state):
        return {
            "slot": owned_name(state, "afs_"),
            "publication": owned_name(state, "afp_"),
        }

    def provision(self, state, target):
        from psycopg import sql

        definition, names = state["definition"], self._names(state)
        relation = definition["postgres_table"].split(".")
        if not 1 <= len(relation) <= 2 or any(not part for part in relation):
            raise ValueError("expected a schema-qualified PostgreSQL table")
        with self.connect(definition["dsn_ref"]) as connection:
            with connection.cursor() as cursor:
                # Serialize publication ownership checks and create within
                # the database, including concurrent same-owner retries.
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (names["slot"],),
                )
                cursor.execute(
                    "SELECT obj_description(oid,'pg_publication') FROM pg_publication WHERE pubname=%s",
                    (names["publication"],),
                )
                existing = cursor.fetchone()
                if existing and existing[0] != state["owner"]:
                    raise Conflict("PostgreSQL publication is not owned by this source")
                if not existing:
                    cursor.execute(
                        sql.SQL("CREATE PUBLICATION {} FOR TABLE {}").format(
                            sql.Identifier(names["publication"]),
                            sql.Identifier(*relation),
                        )
                    )
                    cursor.execute(
                        sql.SQL("COMMENT ON PUBLICATION {} IS {}").format(
                            sql.Identifier(names["publication"]),
                            sql.Literal(state["owner"]),
                        )
                    )
                cursor.execute(
                    "SELECT schemaname,tablename FROM pg_publication_tables WHERE pubname=%s",
                    (names["publication"],),
                )
                expected = tuple(
                    relation if len(relation) == 2 else ["public", relation[0]]
                )
                if cursor.fetchall() != [expected]:
                    raise Conflict("PostgreSQL publication membership changed")
        # The native exported-snapshot workflow creates the slot. Creating it
        # here would destroy the exact snapshot-to-stream cutover guarantee.
        target.configure_postgres(definition, names)
        return names

    def teardown(self, state, target):
        from psycopg import sql

        definition, names = state["definition"], self._names(state)
        target.configure_postgres(definition, names, remove=True)
        with self.connect(definition["dsn_ref"]) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                    (names["slot"],),
                )
                cursor.execute(
                    "SELECT obj_description(oid,'pg_publication') FROM pg_publication WHERE pubname=%s",
                    (names["publication"],),
                )
                publication = cursor.fetchone()
                # Native exported cutovers use authority-specific physical
                # names. The persisted logical publication is their ownership
                # witness; its private prefix belongs exclusively to this role.
                slot_pattern = (
                    "^" + names["slot"][:26] + "_af_[0-9a-f]{16}_[0-9a-f]{16}$"
                )
                publication_pattern = (
                    "^" + names["publication"][:26] + "_af_[0-9a-f]{16}_[0-9a-f]{16}$"
                )
                cursor.execute(
                    "SELECT slot_name,active,plugin,database FROM pg_replication_slots WHERE slot_name=%s OR slot_name ~ %s",
                    (names["slot"], slot_pattern),
                )
                slots = cursor.fetchall()
                cursor.execute(
                    "SELECT pubname,pubowner=(SELECT oid FROM pg_roles WHERE rolname=current_user),obj_description(oid,'pg_publication') FROM pg_publication WHERE pubname ~ %s",
                    (publication_pattern,),
                )
                children = cursor.fetchall()
                if publication is None:
                    if slots or children:
                        raise Conflict(
                            "native resources have no remaining publication ownership witness"
                        )
                    return
                if publication[0] != state["owner"]:
                    raise Conflict("PostgreSQL teardown ownership changed")
                if any(
                    not owned or comment not in (None, state["owner"])
                    for _, owned, comment in children
                ):
                    raise Conflict("native publication owner changed")
                cursor.execute("SELECT current_database()")
                database = cursor.fetchone()[0]
                if any(
                    (plugin, source_database) != ("pgoutput", database)
                    for _, _, plugin, source_database in slots
                ):
                    raise Conflict("slot database or plugin changed")
                if any(active for _, active, _, _ in slots):
                    raise Unavailable("native CDC slot has not drained")
                for slot_name, _, _, _ in slots:
                    cursor.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
                for pubname, _, _ in children:
                    cursor.execute(
                        sql.SQL("DROP PUBLICATION {}").format(sql.Identifier(pubname))
                    )
                cursor.execute(
                    sql.SQL("DROP PUBLICATION {}").format(
                        sql.Identifier(names["publication"])
                    )
                )

    def observe(self, state, target):
        definition = state["definition"]
        current = target.configuration(definition["table"])
        if current["table_id"] != definition["table_id"]:
            raise Conflict("CDC destination incarnation changed")
        name = self._names(state)["slot"]
        ordinal = next(
            (
                index
                for index, source in enumerate(current["replication_sources"])
                if source.get("slot_name") == name
            ),
            None,
        )
        if ordinal is None:
            raise Conflict("native CDC configuration was removed outside its owner")
        return [
            item
            for item in current.get("progress", [])
            if item["source_ordinal"] == ordinal
        ]


class S3Notifications:
    def __init__(self, s3, sqs, *, verify_exclusive_configuration_writer):
        # S3 notification replacement has no compare-and-swap. An enforced
        # configuration-writer policy is a prerequisite, not a lease heuristic.
        self.s3, self.sqs = s3, sqs
        self.verify_exclusive_writer = verify_exclusive_configuration_writer

    def _queue(self, state, create=False):
        name = owned_name(state, "antfly-")
        try:
            url = self.sqs.get_queue_url(QueueName=name)["QueueUrl"]
        except self.sqs.exceptions.QueueDoesNotExist:
            if not create:
                raise
            url = self.sqs.create_queue(
                QueueName=name, tags={"antfly-owner": state["owner"]}
            )["QueueUrl"]
        tags = self.sqs.list_queue_tags(QueueUrl=url).get("Tags", {})
        if tags.get("antfly-owner") != state["owner"]:
            raise Conflict("SQS queue is not owned by this source")
        arn = self.sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["QueueArn"])[
            "Attributes"
        ]["QueueArn"]
        return url, arn

    def provision(self, state, target):
        definition = state["definition"]
        self.verify_exclusive_writer(definition["bucket"])
        url, arn = self._queue(state, True)
        bucket_arn = "arn:aws:s3:::" + definition["bucket"]
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "s3.amazonaws.com"},
                    "Action": "sqs:SendMessage",
                    "Resource": arn,
                    "Condition": {
                        "ArnEquals": {"aws:SourceArn": bucket_arn},
                        "StringEquals": {"aws:SourceAccount": definition["account_id"]},
                    },
                }
            ],
        }
        self.sqs.set_queue_attributes(
            QueueUrl=url, Attributes={"Policy": json.dumps(policy)}
        )
        current = self.s3.get_bucket_notification_configuration(
            Bucket=definition["bucket"]
        )
        current.pop("ResponseMetadata", None)
        configurations = current.setdefault("QueueConfigurations", [])
        identifier = owned_name(state, "antfly-")
        wanted = {
            "Id": identifier,
            "QueueArn": arn,
            "Events": ["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
            "Filter": {
                "Key": {
                    "FilterRules": [
                        {"Name": "prefix", "Value": definition.get("prefix", "")}
                    ]
                }
            },
        }
        existing = [item for item in configurations if item.get("Id") == identifier]
        if existing and existing != [wanted]:
            raise Conflict("S3 notification was modified outside its owner")
        if not existing:
            configurations.append(wanted)
            self.s3.put_bucket_notification_configuration(
                Bucket=definition["bucket"], NotificationConfiguration=current
            )
        return {"queue_url": url, "queue_arn": arn, "notification_id": identifier}

    def poll(self, state):
        url, _ = self._queue(state)
        return self.sqs.receive_message(
            QueueUrl=url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
            VisibilityTimeout=120,
        ).get("Messages", [])

    def ack(self, state, messages):
        if messages:
            url, _ = self._queue(state)
            result = self.sqs.delete_message_batch(
                QueueUrl=url,
                Entries=[
                    {"Id": str(index), "ReceiptHandle": item["ReceiptHandle"]}
                    for index, item in enumerate(messages)
                ],
            )
            if result.get("Failed"):
                raise Unavailable("SQS acknowledgement incomplete")

    def teardown(self, state, target):
        definition = state["definition"]
        self.verify_exclusive_writer(definition["bucket"])
        current = self.s3.get_bucket_notification_configuration(
            Bucket=definition["bucket"]
        )
        current.pop("ResponseMetadata", None)
        identifier = owned_name(state, "antfly-")
        ours = [
            item
            for item in current.get("QueueConfigurations", [])
            if item.get("Id") == identifier
        ]
        try:
            url, arn = self._queue(state)
        except self.sqs.exceptions.QueueDoesNotExist:
            if ours:
                raise Conflict("notification remains but owned queue is missing")
            return
        if any(item["QueueArn"] != arn for item in ours):
            raise Conflict("notification points at another queue")
        if ours:
            current["QueueConfigurations"] = [
                item
                for item in current["QueueConfigurations"]
                if item.get("Id") != identifier
            ]
            self.s3.put_bucket_notification_configuration(
                Bucket=definition["bucket"], NotificationConfiguration=current
            )
        self.sqs.delete_queue(QueueUrl=url)


def exclusive_s3_writer(s3, sts, role_arn):
    """Verify a concrete bucket-policy fence; never provision a blanket IAM grant."""
    caller = sts.get_caller_identity()
    account = role_arn.split(":")[4]
    role = role_arn.rsplit("/", 1)[-1]
    expected_session = f"arn:aws:sts::{account}:assumed-role/{role}/"
    if caller["Account"] != account or not (
        caller["Arn"] == role_arn or caller["Arn"].startswith(expected_session)
    ):
        raise PermissionError("configuration credentials are not the enforced owner")

    def verify(bucket):
        policy = json.loads(s3.get_bucket_policy(Bucket=bucket)["Policy"])
        required = {
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:PutBucketNotification",
            "Resource": "arn:aws:s3:::" + bucket,
            "Condition": {"ArnNotEquals": {"aws:PrincipalArn": role_arn}},
        }
        for statement in policy.get("Statement", []):
            candidate = {key: value for key, value in statement.items() if key != "Sid"}
            if candidate == required:
                return
        raise PermissionError(
            "bucket lacks the exclusive notification configuration policy"
        )

    return verify


class GCSNotifications:
    def __init__(self, storage, publisher, subscriber):
        self.storage, self.publisher, self.subscriber = storage, publisher, subscriber

    def _paths(self, state):
        project = state["definition"]["project"]
        name = owned_name(state, "antfly-")
        return self.publisher.topic_path(
            project, name
        ), self.subscriber.subscription_path(project, name)

    def _labels(self, state):
        return {"antfly-owner": state["owner"][:63]}

    def provision(self, state, target):
        from google.api_core.exceptions import AlreadyExists

        definition = state["definition"]
        topic, subscription = self._paths(state)
        labels = self._labels(state)
        for client, create, get, resource in (
            (
                self.publisher,
                "create_topic",
                "get_topic",
                {"name": topic, "labels": labels},
            ),
            (
                self.subscriber,
                "create_subscription",
                "get_subscription",
                {
                    "name": subscription,
                    "topic": topic,
                    "labels": labels,
                    "ack_deadline_seconds": 120,
                },
            ),
        ):
            try:
                getattr(client, create)(request=resource)
            except AlreadyExists:
                pass
            saved = getattr(client, get)(
                request={
                    "topic" if get == "get_topic" else "subscription": resource["name"]
                }
            )
            if saved.labels.get("antfly-owner") != labels["antfly-owner"]:
                raise Conflict("Pub/Sub resource is not owned by this source")
        agent = self.storage.get_service_account_email(
            project=definition.get("bucket_project", definition["project"])
        )
        # Preserve other bindings and the provider etag. A concurrent policy
        # edit fails rather than replacing its grants.
        policy = self.publisher.get_iam_policy(request={"resource": topic})
        role = "roles/pubsub.publisher"
        binding = next((entry for entry in policy.bindings if entry.role == role), None)
        member = "serviceAccount:" + agent
        if binding is None:
            policy.bindings.add(role=role, members=[member])
        elif member not in binding.members:
            binding.members.append(member)
        self.publisher.set_iam_policy(request={"resource": topic, "policy": policy})
        bucket = self.storage.bucket(definition["bucket"])
        attributes = {
            "antfly-owner": state["owner"],
            "antfly-definition": state["fingerprint"],
        }
        ours = [
            entry
            for entry in bucket.list_notifications()
            if entry.custom_attributes == attributes
        ]
        for entry in ours:
            if (
                entry.topic_name != owned_name(state, "antfly-")
                or entry.topic_project != definition["project"]
            ) or entry.blob_name_prefix != definition.get("prefix", ""):
                raise Conflict("GCS notification was modified outside its owner")
        if not ours:
            entry = bucket.notification(
                topic_name=owned_name(state, "antfly-"),
                topic_project=definition["project"],
                payload_format="JSON_API_V1",
                custom_attributes=attributes,
                event_types=["OBJECT_FINALIZE", "OBJECT_DELETE"],
                blob_name_prefix=definition.get("prefix", ""),
            )
            entry.create()
            ours = [entry]
        return {
            "topic": topic,
            "subscription": subscription,
            "notifications": [entry.notification_id for entry in ours],
        }

    def poll(self, state):
        from google.api_core.exceptions import DeadlineExceeded

        _, subscription = self._paths(state)
        saved = self.subscriber.get_subscription(
            request={"subscription": subscription}, timeout=10, retry=None
        )
        if saved.labels.get("antfly-owner") != self._labels(state)["antfly-owner"]:
            raise Conflict("Pub/Sub polling ownership changed")
        try:
            return list(
                self.subscriber.pull(
                    request={"subscription": subscription, "max_messages": 10},
                    timeout=5,
                    retry=None,
                ).received_messages
            )
        except DeadlineExceeded:
            # An empty long poll still needs periodic native reconciliation.
            # No message has been acknowledged, so a late delivery is replayable.
            return []

    def ack(self, state, messages):
        if messages:
            _, subscription = self._paths(state)
            self.subscriber.acknowledge(
                request={
                    "subscription": subscription,
                    "ack_ids": [entry.ack_id for entry in messages],
                }
            )

    def teardown(self, state, target):
        from google.api_core.exceptions import NotFound

        topic, subscription = self._paths(state)
        labels = self._labels(state)
        # Verify both resources before deleting any of the provider graph.
        resources = []
        for client, method, argument, path, delete in (
            (
                self.subscriber,
                "get_subscription",
                "subscription",
                subscription,
                "delete_subscription",
            ),
            (self.publisher, "get_topic", "topic", topic, "delete_topic"),
        ):
            try:
                saved = getattr(client, method)(request={argument: path})
            except NotFound:
                continue
            if saved.labels.get("antfly-owner") != labels["antfly-owner"]:
                raise Conflict("Pub/Sub teardown ownership changed")
            resources.append((client, delete, argument, path))
        bucket = self.storage.bucket(state["definition"]["bucket"])
        for entry in bucket.list_notifications():
            if entry.custom_attributes.get("antfly-owner") == state["owner"]:
                if (
                    entry.topic_name != owned_name(state, "antfly-")
                    or entry.topic_project != state["definition"]["project"]
                ):
                    raise Conflict("GCS notification teardown ownership changed")
                entry.delete()
        for client, method, argument, path in resources:
            try:
                getattr(client, method)(request={argument: path})
            except NotFound:
                pass
