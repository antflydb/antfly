# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Durable admission, immutable retirement and resumable physical deletion."""

import json
import time
import uuid
from dataclasses import asdict
from urllib.parse import unquote, urlsplit

from pydantic import TypeAdapter
from pyiceberg.table.update import TableRequirement, TableUpdate, update_table_metadata
from pyiceberg.table.metadata import TableMetadataUtil

from .provider import Provider, Reachability
from .store import Conflict, Unavailable, digest, encode


class Controller:
    def __init__(self, config, store, *, now_ns=time.time_ns):
        self.config, self.store, self.now_ns = config, store, now_ns
        self.provider = Provider(config, store)
        if config["provider"] not in ("nessie", "polaris"):
            raise ValueError("unknown provider")
        for field in ("authority_uri", "warehouse_uri", "artifact_uri"):
            if not config[field].endswith("/"):
                raise ValueError(f"{field} must end in slash")
        roots = [
            config[key] for key in ("authority_uri", "warehouse_uri", "artifact_uri")
        ]
        for index, root in enumerate(roots):
            self.store._parts(root)
            if any(
                root.startswith(other) or other.startswith(root)
                for other in roots[index + 1 :]
            ):
                raise ValueError(
                    "warehouse, authority and artifact roots must be disjoint"
                )
        if not config.get("gateway_enforced"):
            raise ValueError(
                "vendor network and credentials must enforce gateway-only access"
            )
        identity = {
            key: config.get(key)
            for key in (
                "provider",
                "catalog_uri",
                "upstream_uri",
                "nessie_uri",
                "warehouse",
                "warehouse_uri",
                "artifact_uri",
                "artifact_connection",
            )
        }
        self.store.require_versioned_deletion(config["warehouse_uri"])
        self.store.probe_authority(config["authority_uri"])
        self.store.immutable(self.key("binding.json"), encode(identity))

    def key(self, path):
        return self.config["authority_uri"] + path

    def state(self):
        value = self.store.get(self.key("HEAD.json"))
        return (
            json.loads(value[0])
            if value
            else {"writer": None, "vacuum": None, "readers": {}}
        )

    def capabilities(self):
        provider = self.config["provider"]
        return {
            "protocol": 1,
            "provider": provider,
            "catalog_uri": self.config["catalog_uri"],
            "writer_fencing": True,
            "external_reader_protection": True,
            "native_reader_registry": True,
            "immutable_retirement": True,
            "idempotent_jobs": True,
            "nessie_references": provider == "nessie",
            "polaris_table_roots": provider == "polaris",
        }

    def graph(self, *, reject_retired=False):
        return Reachability(
            self.provider,
            self.config["warehouse_uri"],
            max_files=self.config.get("max_files", 100_000),
            max_bytes=self.config.get("max_metadata_bytes", 256 * 1024 * 1024),
            retirement_check=self.reject_retired if reject_retired else None,
        )

    def reject_retired(self, files):
        for uri in files:
            if self.store.get(self.key("retired/" + digest(uri.encode()))) is not None:
                raise Conflict("commit references an irreversibly retired file")

    def acquire_reader(self, namespace, name, *, ttl_ms=120_000, input_files=()):
        if not 1_000 <= ttl_ms <= 3_600_000:
            raise ValueError("reader lease must be between one second and one hour")
        current = self.provider.load(namespace, name)
        graph = self.graph()
        metadata = graph.mark(current["metadata-location"])
        self.reject_retired(graph.files)
        for uri in input_files:
            graph.check_uri(uri)
        identifier = uuid.uuid4().hex
        lease = {
            "id": identifier,
            "namespace": namespace,
            "name": name,
            "table_uuid": metadata["table-uuid"],
            "metadata_location": current["metadata-location"],
            "expires_ns": self.now_ns() + ttl_ms * 1_000_000,
            "input_files": list(input_files),
        }

        def admit(state):
            if state["vacuum"] or state["writer"]:
                raise Conflict("catalog admission is fenced")
            # The load occurred outside admission. Writer exclusion plus an
            # authoritative reread below prevents pinning a raced incarnation.
            if len(state["readers"]) >= self.config.get("max_readers", 1024):
                expired = [
                    key
                    for key, value in state["readers"].items()
                    if value["expires_ns"] + 30_000_000_000 < self.now_ns()
                ]
                for key in expired:
                    del state["readers"][key]
            if len(state["readers"]) >= self.config.get("max_readers", 1024):
                raise Unavailable("reader admission budget exceeded")
            state["readers"][identifier] = lease

        self.store.mutate(self.key("HEAD.json"), admit)
        latest = self.provider.load(namespace, name)
        if latest["metadata-location"] != current["metadata-location"]:
            self.release_reader(identifier)
            raise Conflict("catalog changed during reader admission")
        # A concurrent vacuum must see this pin, or win admission before it.
        return lease

    def renew_reader(self, identifier, ttl_ms=120_000):
        if not 1_000 <= ttl_ms <= 3_600_000:
            raise ValueError("invalid reader duration")

        def renew(state):
            lease = state["readers"].get(identifier)
            if lease is None or lease["expires_ns"] <= self.now_ns():
                raise Conflict("reader lease expired")
            lease["expires_ns"] = max(
                lease["expires_ns"], self.now_ns() + ttl_ms * 1_000_000
            )
            return dict(lease)

        return self.store.mutate(self.key("HEAD.json"), renew)

    def release_reader(self, identifier):
        self.store.mutate(
            self.key("HEAD.json"), lambda state: state["readers"].pop(identifier, None)
        )

    def leased_table(self, identifier, namespace, name):
        lease = self.state()["readers"].get(identifier)
        if (
            not lease
            or lease["expires_ns"] <= self.now_ns()
            or lease["namespace"] != namespace
            or lease["name"] != name
        ):
            raise Conflict("missing or expired external reader lease")
        metadata = self.graph().read_metadata(lease["metadata_location"])
        if metadata["table-uuid"] != lease["table_uuid"]:
            raise Conflict("reader table incarnation changed")
        return {"metadata": metadata, "metadata-location": lease["metadata_location"]}

    def _table(self, path):
        parts = urlsplit(path).path.split("/")
        if "namespaces" not in parts or "tables" not in parts:
            return None
        offset = parts.index("namespaces")
        if len(parts) == offset + 4 and parts[offset + 2] == "tables":
            return unquote(parts[offset + 1]).split("\x1f"), unquote(parts[-1])
        return None

    def proxy_write(self, method, path, body, *, native=False, grant_lease=False):
        """Only one unresolved provider write; unknown outcomes never expire."""
        operation = uuid.uuid4().hex
        payload = json.loads(body) if body else {}
        intent = {
            "operation": operation,
            "method": method,
            "path": path,
            "native": native,
            "body": payload,
        }
        self.store.immutable(self.key(f"writes/{operation}.json"), encode(intent))

        def admit(state):
            if state["writer"] or state["vacuum"]:
                raise Conflict("catalog mutation is fenced")
            state["writer"] = {"operation": operation, "phase": "admitted"}

        self.store.mutate(self.key("HEAD.json"), admit)
        sent = False
        try:
            if native:
                graph = self.graph(reject_retired=True)
                if method == "POST" and path.endswith("/history/commit"):
                    for op in payload.get("operations", []):
                        content = op.get("content", {})
                        if content.get("type") == "ICEBERG_TABLE":
                            graph.mark(content["metadataLocation"])
                        elif content.get("type") not in (None, "NAMESPACE"):
                            raise ValueError("unsupported native content type")
                    payload.setdefault("commitMeta", {}).setdefault("properties", {})[
                        "antfly.gateway.commit"
                    ] = operation
                elif (
                    method == "POST"
                    and urlsplit(path).path == "/trees"
                    or method == "PUT"
                    and urlsplit(path).path.startswith("/trees/")
                ):
                    from urllib.parse import quote

                    source = payload.get("hash")
                    if not source:
                        raise ValueError(
                            "reference changes require an explicit source hash"
                        )
                    if method == "PUT" and "@" not in unquote(urlsplit(path).path):
                        raise ValueError(
                            "reference assignment requires an expected hash"
                        )
                    for entry in self.provider._pages(
                        "/trees/@" + quote(source, safe="") + "/entries?content=true",
                        "entries",
                        native=True,
                    ):
                        content = entry.get("content") or {}
                        if content.get("type") == "ICEBERG_TABLE":
                            graph.mark(content["metadataLocation"])
                        elif content.get("type") != "NAMESPACE":
                            raise ValueError("unsupported referenced content type")
                elif (
                    method == "DELETE"
                    and urlsplit(path).path.startswith("/trees/")
                    and "@" in unquote(urlsplit(path).path)
                ):
                    pass
                else:
                    raise ValueError(
                        "unsupported native mutation; use fenced commits or reference changes"
                    )
                self.reject_retired(graph.files)
            elif table := self._table(path):
                if method != "POST":
                    raise ValueError(
                        "table drop/purge is not allowed through the maintenance gateway"
                    )
                current = self.provider.load(*table)
                metadata = TableMetadataUtil.parse_obj(current["metadata"])
                requirements = TypeAdapter(list[TableRequirement]).validate_python(
                    payload.get("requirements", [])
                )
                for requirement in requirements:
                    requirement.validate(metadata)
                updates = TypeAdapter(list[TableUpdate]).validate_python(
                    payload.get("updates", [])
                )
                for update in payload.get("updates", []):
                    if update.get("action") == "set-properties" and any(
                        key.startswith("antfly.gateway.")
                        for key in update.get("updates", {})
                    ):
                        raise ValueError("reserved gateway property")
                proposed = update_table_metadata(
                    metadata, tuple(updates), enforce_validation=True
                ).model_dump(by_alias=True, exclude_none=True)
                graph = self.graph(reject_retired=True)
                graph.mark_values(proposed)
                self.reject_retired(graph.files)
                payload.setdefault("updates", []).append(
                    {
                        "action": "set-properties",
                        "updates": {"antfly.gateway.commit": operation},
                    }
                )
            elif method == "POST" and path.rstrip("/").endswith("/tables"):
                # Table creation: the catalog writes the initial metadata.
                if payload.get("stage-create"):
                    raise ValueError("staged creation requires explicit input leasing")
                if payload.get("location"):
                    self.graph(reject_retired=True).check_uri(
                        payload["location"].rstrip("/") + "/"
                    )
                payload.setdefault("properties", {})["antfly.gateway.commit"] = (
                    operation
                )
            elif method == "POST" and path.rstrip("/").endswith("/namespaces"):
                payload.setdefault("properties", {})["antfly.gateway.commit"] = (
                    operation
                )
            else:
                raise ValueError("unsupported catalog mutation; use table commits")

            def send_admission(state):
                if (
                    not state["writer"]
                    or state["writer"]["operation"] != operation
                    or state["writer"]["phase"] != "admitted"
                ):
                    raise Conflict("writer admission changed before dispatch")
                state["writer"]["phase"] = "sent"

            self.store.mutate(self.key("HEAD.json"), send_admission)
            sent = True
            status, data = self.provider.request(
                method, path, encode(payload), native=native
            )
            lease = None
            if 200 <= status < 300 and grant_lease and not native and data:
                response = json.loads(data)
                if "metadata-location" in response:
                    target = self._table(path)
                    if target is None:
                        parts = urlsplit(path).path.split("/")
                        offset = parts.index("namespaces")
                        target = (
                            unquote(parts[offset + 1]).split("\x1f"),
                            payload["name"],
                        )
                    lease = {
                        "id": uuid.uuid4().hex,
                        "namespace": target[0],
                        "name": target[1],
                        "table_uuid": response["metadata"]["table-uuid"],
                        "metadata_location": response["metadata-location"],
                        "expires_ns": self.now_ns() + 120_000_000_000,
                        "input_files": [],
                    }
                    response["antfly-reader-lease"] = lease
                    response.pop("storage-credentials", None)
                    response["config"] = {}
                    response.setdefault("config", {})["antfly.reader-lease"] = (
                        json.dumps(lease)
                    )
                    data = encode(response)
            if 200 <= status < 300 or 400 <= status < 500 and status not in (408, 429):
                self._finish_writer(operation, lease)
            return status, data
        except Exception:
            if not sent:
                self._finish_writer(operation)
            raise

    def _finish_writer(self, operation, lease=None):
        def finish(state):
            if not state["writer"] or state["writer"]["operation"] != operation:
                raise Conflict("writer admission changed")
            if lease is not None:
                if len(state["readers"]) >= self.config.get("max_readers", 1024):
                    raise Unavailable("commit reader admission budget exceeded")
                state["readers"][lease["id"]] = lease
            state["writer"] = None

        self.store.mutate(self.key("HEAD.json"), finish)

    def recover_writer(self):
        admission = self.state()["writer"]
        if admission is None:
            return {"complete": True}
        operation = admission["operation"]
        if admission["phase"] == "admitted":

            def release_unsent(state):
                if state["writer"] != admission:
                    raise Conflict("writer advanced during recovery")
                state["writer"] = None

            self.store.mutate(self.key("HEAD.json"), release_unsent)
            return {"complete": True, "operation": operation}
        intent = json.loads(self.store.get(self.key(f"writes/{operation}.json"))[0])
        table = self._table(intent["path"])
        if (
            table is None
            and not intent["native"]
            and intent["path"].endswith("/tables")
        ):
            parts = urlsplit(intent["path"]).path.split("/")
            offset = parts.index("namespaces")
            table = (unquote(parts[offset + 1]).split("\x1f"), intent["body"]["name"])
        if not intent["native"] and table:
            current = self.provider.load(*table)
            if (
                current["metadata"].get("properties", {}).get("antfly.gateway.commit")
                == operation
            ):
                self._finish_writer(operation)
                return {"complete": True, "operation": operation}
        if intent["native"] and intent["path"].endswith("/history/commit"):
            from urllib.parse import quote

            branch = unquote(
                intent["path"].split("/trees/", 1)[1].split("/history/", 1)[0]
            ).split("@", 1)[0]
            history = self.provider.json(
                "GET",
                "/trees/" + quote(branch, safe="") + "/history?fetch=ALL",
                native=True,
            )
            if any(
                entry.get("commitMeta", {})
                .get("properties", {})
                .get("antfly.gateway.commit")
                == operation
                for entry in history.get("logEntries", [])
            ):
                self._finish_writer(operation)
                return {"complete": True, "operation": operation}
        elif not intent["native"] and intent["path"].endswith("/namespaces"):
            from urllib.parse import quote

            path = (
                intent["path"]
                + "/"
                + quote("\x1f".join(intent["body"]["namespace"]), safe="")
            )
            namespace = self.provider.json("GET", path)
            if (
                namespace.get("properties", {}).get("antfly.gateway.commit")
                == operation
            ):
                self._finish_writer(operation)
                return {"complete": True, "operation": operation}
        # Never infer failure from an absent marker: the vendor may still be
        # executing the original request. Operator reconciliation must first
        # fence/drain that provider request, not just expire an ownership lease.
        return {"complete": False, "operation": operation}

    def _job_registry(self, job):
        registry = job["reader_registry"]
        parsed = urlsplit(self.config["artifact_uri"])
        prefix = (
            parsed.path.strip("/")
            + "/lake-readers/"
            + digest(encode([job["source_uri"], job["table_uuid"]]))
        )
        if (
            registry.get("protocol") != "antfly-snapshot-pins-v1"
            or registry["connection"] != self.config["artifact_connection"]
            or registry["bucket"] != parsed.netloc
            or registry["prefix"] != prefix
            or registry.get("lease_grace_ms") != 30_000
        ):
            raise ValueError("job reader registry does not match configured authority")
        return f"{parsed.scheme}://{parsed.netloc}/{prefix}/snapshots/"

    def _native_pins(self, registry):
        active = set()
        count = 0
        for obj in self.store.inventory(registry + "pins/"):
            count += 1
            if count > self.config.get("max_readers", 1024):
                raise Unavailable("native reader inventory budget exceeded")
            result = self.store.get(obj.uri)
            if result is None:
                raise Unavailable("native reader inventory changed")
            if int(result[0]) + 30_000_000_000 >= self.now_ns():
                active.add(obj.uri.rsplit("/", 1)[-1])
        return active

    def run_job(self, request_hash, body):
        if digest(body) != request_hash:
            raise ValueError("job hash mismatch")
        job = json.loads(body)
        policy = job["policy"]
        if (
            job["protocol"] != 1
            or job["provider"] != self.config["provider"]
            or job["catalog"]["uri"] != self.config["catalog_uri"]
            or job["catalog"].get("warehouse") != self.config.get("warehouse")
        ):
            raise ValueError("job catalog binding mismatch")
        if (
            any(
                type(policy.get(key)) is not int
                for key in ("max_deleted", "keep_latest", "retain_ms")
            )
            or type(policy.get("dry_run")) is not bool
        ):
            raise ValueError(
                "maintenance limits must be integers and dry_run a boolean"
            )
        if (
            not 0 < len(job["operation_id"]) <= 256
            or not 1 <= policy["max_deleted"] <= 4096
            or not 1 <= policy["keep_latest"] <= 1024
            or policy["retain_ms"] < 600_000
        ):
            raise ValueError("invalid maintenance limits")
        registry = self._job_registry(job)
        self.store.immutable(self.key(f"jobs/{request_hash}/intent.json"), body)
        receipt_uri = self.key(f"jobs/{request_hash}/receipt.json")
        if saved := self.store.get(receipt_uri):
            owner = self.state()["vacuum"]
            if owner and owner["id"] == request_hash:
                self._release_job(request_hash)
            return json.loads(saved[0])
        base = {
            "protocol": 1,
            "provider": job["provider"],
            "operation_id": job["operation_id"],
            "request_hash": request_hash,
            "table_uuid": job["table_uuid"],
            "state": "running",
            "expired_snapshots": 0,
            "eligible_objects": 0,
            "deleted_objects": 0,
            "retained_objects": 0,
        }

        def admit(state):
            if (
                state["writer"]
                or state["vacuum"] is not None
                and state["vacuum"]["id"] != request_hash
            ):
                raise Conflict("maintenance awaits catalog quiescence")
            if state["vacuum"] is None:
                state["vacuum"] = {
                    "id": request_hash,
                    "phase": "planning",
                    "epoch": uuid.uuid4().hex,
                }
            return dict(state["vacuum"])

        admission = self.store.mutate(self.key("HEAD.json"), admit)
        plan_uri = self.key(f"jobs/{request_hash}/plan.json")
        saved = self.store.get(plan_uri)
        if saved:
            plan = json.loads(saved[0])
        else:
            # Planning errors release admission because no provider mutation or
            # retirement has occurred. Published plans retain the durable fence.
            try:
                plan = self._plan(job, registry, base)
                plan["admission_epoch"] = admission["epoch"]
                self.store.immutable(plan_uri, encode(plan))
            except Exception:
                if self.store.get(plan_uri) is None:
                    self._release_job(
                        request_hash, planning_only=True, epoch=admission["epoch"]
                    )
                raise

        if plan["admission_epoch"] != admission["epoch"]:
            self._release_job(
                request_hash, planning_only=True, epoch=admission["epoch"]
            )
            raise Conflict("planning admission was revoked; submit a new operation")

        def prepare(state):
            if (
                state["vacuum"] is None
                or state["vacuum"]["id"] != request_hash
                or state["vacuum"]["epoch"] != plan["admission_epoch"]
            ):
                raise Conflict("maintenance admission changed")
            state["vacuum"]["phase"] = "prepared"

        self.store.mutate(self.key("HEAD.json"), prepare)
        if policy["dry_run"]:
            result = {
                **base,
                **plan["counts"],
                "state": "complete",
                "deleted_objects": 0,
            }
            self.store.immutable(receipt_uri, encode(result))
            self._release_job(request_hash)
            return result
        current = self.provider.load(
            job["catalog"]["namespace"], job["catalog"]["name"]
        )
        marker = current["metadata"].get("properties", {}).get("antfly.maintenance.job")
        if marker != request_hash:
            if (
                current["metadata-location"] != plan["metadata_location"]
                or current["metadata"]["table-uuid"] != job["table_uuid"]
            ):
                raise Conflict("maintenance authority changed")
            # Native readers check these markers before and after publishing
            # their pin. Retire before the final pin check or metadata commit.
            for snapshot in plan["retired_snapshots"]:
                self.store.immutable(
                    registry + "retired/" + digest(snapshot.encode()), snapshot.encode()
                )
            if self._native_pins(registry) & {
                digest(item.encode()) for item in plan["retired_snapshots"]
            }:
                return {**base, "state": "running"}
            self.provider.commit_expiration(
                job["catalog"]["namespace"],
                job["catalog"]["name"],
                current,
                plan["remove_snapshots"],
                request_hash,
            )
        # HEAD still excludes every writer/new reader. Irreversible URI markers
        # fence later writes even after admission reopens or the owner restarts.
        remaining = []
        deleted = 0
        for candidate in plan["objects"]:
            key = digest(encode([candidate["uri"], candidate["version"]]))
            done_uri = self.key(f"jobs/{request_hash}/deleted/{key}")
            if self.store.get(done_uri):
                deleted += 1
            else:
                remaining.append((candidate, done_uri))
        self.store.require_versioned_deletion(self.config["warehouse_uri"])
        for candidate, done_uri in remaining[: self.config.get("delete_batch", 64)]:
            self.store.immutable(
                self.key("retired/" + digest(candidate["uri"].encode())),
                candidate["uri"].encode(),
            )
            self.store.delete(candidate["uri"], candidate["version"])
            self.store.immutable(done_uri, b"deleted")
            deleted += 1
        complete = deleted == len(plan["objects"])
        result = {
            **base,
            **plan["counts"],
            "state": "complete" if complete else "running",
            "deleted_objects": deleted if complete else 0,
        }
        if complete:
            self.store.immutable(receipt_uri, encode(result))
            self._release_job(request_hash)
        return result

    def _release_job(self, request_hash, *, planning_only=False, epoch=None):
        def release(state):
            if (
                epoch is not None
                and state["vacuum"] is not None
                and state["vacuum"].get("epoch") != epoch
            ):
                raise Conflict("maintenance planning epoch changed")
            if state["vacuum"] is not None and state["vacuum"]["id"] != request_hash:
                raise Conflict("maintenance owner changed")
            if (
                planning_only
                and state["vacuum"] is not None
                and state["vacuum"]["phase"] != "planning"
            ):
                raise Conflict("maintenance plan is already prepared")
            state["vacuum"] = None

        self.store.mutate(self.key("HEAD.json"), release)

    def _plan(self, job, registry, base):
        namespace, name = job["catalog"]["namespace"], job["catalog"]["name"]
        current = self.provider.load(namespace, name)
        metadata = current["metadata"]
        if (
            metadata["table-uuid"] != job["table_uuid"]
            or metadata["location"].rstrip("/") != job["source_uri"].rstrip("/")
            or current["metadata-location"] != job["expected_metadata_location"]
        ):
            raise Conflict("stale maintenance table incarnation or metadata")
        graph = self.graph()
        graph.history(current["metadata-location"])
        native_history = {key: list(value) for key, value in graph.snapshots.items()}
        protected = set(map(str, job["protected_snapshots"]))
        native_pins = self._native_pins(registry)
        known = {
            digest(identifier.encode()): identifier for identifier in graph.snapshots
        }
        if native_pins - known.keys():
            raise Unavailable("active native pin has no recoverable metadata root")
        protected.update(known[key] for key in native_pins)
        snapshots = sorted(
            metadata.get("snapshots", []),
            key=lambda value: (value["timestamp-ms"], value["snapshot-id"]),
        )
        policy = job["policy"]
        before = self.now_ns() // 1_000_000 - policy["retain_ms"]
        protected.update(
            str(value["snapshot-id"]) for value in snapshots[-policy["keep_latest"] :]
        )
        protected.update(
            str(value["snapshot-id"])
            for value in snapshots
            if value["timestamp-ms"] >= before
        )
        protected.update(
            str(value["snapshot-id"]) for value in metadata.get("refs", {}).values()
        )
        if metadata.get("current-snapshot-id", -1) != -1:
            protected.add(str(metadata["current-snapshot-id"]))
        if protected - graph.snapshots.keys():
            raise Unavailable(
                "protected publication snapshot is not in metadata history"
            )
        remove = [
            value["snapshot-id"]
            for value in snapshots
            if str(value["snapshot-id"]) not in protected
        ]
        # All roots across the provider, including other tables sharing files.
        seen_roots = set()
        for root in self.provider.all_metadata():
            if root in seen_roots:
                continue
            seen_roots.add(root)
            if len(seen_roots) > self.config.get("max_files", 100_000):
                raise Unavailable("provider root budget exceeded")
            if (
                root == current["metadata-location"]
                and self.config["provider"] == "polaris"
            ):
                graph.mark(root, snapshots=protected)
            else:
                root_metadata = graph.mark(root)
                # Another native table can retain a historical snapshot whose
                # files happen to live under this table's prefix. Protect that
                # registry too; current vendor roots alone are insufficient.
                other_registry = (
                    self.config["artifact_uri"]
                    + "lake-readers/"
                    + digest(
                        encode([root_metadata["location"], root_metadata["table-uuid"]])
                    )
                    + "/snapshots/"
                )
                other_pins = self._native_pins(other_registry)
                if other_pins:
                    other_graph = self.graph()
                    other_graph.history(root)
                    other_known = {
                        digest(key.encode()): key for key in other_graph.snapshots
                    }
                    if other_pins - other_known.keys():
                        raise Unavailable(
                            "other table's active native pin lacks a metadata root"
                        )
                    for pin in other_pins:
                        for pin_root, snapshot in other_graph.snapshots[
                            other_known[pin]
                        ]:
                            graph.add(pin_root)
                            graph.mark_snapshot(snapshot)
        for identifier in protected:
            for root, snapshot in graph.snapshots[identifier]:
                graph.add(root)
                graph.mark_snapshot(snapshot)
        for lease in self.state()["readers"].values():
            if lease["expires_ns"] + 30_000_000_000 >= self.now_ns():
                graph.mark(lease["metadata_location"])
                for uri in lease["input_files"]:
                    graph.add(uri)
        graph.mark(current["metadata-location"], snapshots=protected)
        prefix = metadata["location"].rstrip("/") + "/"
        graph.check_uri(prefix)
        if prefix == self.config["warehouse_uri"]:
            raise Unavailable("cannot vacuum an entire warehouse as one table")
        selected, count, retained = [], 0, 0
        for obj in self.store.inventory(prefix, all_versions=True):
            count += 1
            if count > self.config.get("max_files", 100_000):
                raise Unavailable("table inventory budget exceeded")
            if obj.uri in graph.files or obj.modified_ms >= before:
                retained += 1
            elif len(selected) < policy["max_deleted"]:
                selected.append(asdict(obj))
        # Nessie retains all content history: do not expire snapshots whose
        # history remains publicly addressable through native catalog refs.
        if self.config["provider"] == "nessie":
            remove = []
        retired = [
            identifier
            for identifier, roots in native_history.items()
            if not any(
                snapshot.get("manifest-list") in graph.live_snapshots
                for _, snapshot in roots
            )
        ]
        retired.extend(str(value) for value in remove)
        return {
            "metadata_location": current["metadata-location"],
            "objects": selected,
            "remove_snapshots": remove,
            "retired_snapshots": sorted(set(retired)),
            "counts": {
                "expired_snapshots": len(remove),
                "eligible_objects": len(selected),
                "retained_objects": retained,
            },
        }
