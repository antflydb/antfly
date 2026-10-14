# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Actual provider discovery and Iceberg file reachability, not protocol fixtures."""

import json
import threading
import time
from urllib.error import HTTPError
from urllib.parse import quote, unquote, urlencode, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import read_manifest_list
from pyiceberg.table.metadata import TableMetadataUtil
from pyiceberg.table.update import RemoveSnapshotsUpdate, SetPropertiesUpdate

from .store import Conflict, Unavailable, digest, encode


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class Provider:
    def __init__(self, config, store):
        self.config, self.store = config, store
        self.io = PyArrowFileIO(config.get("io_properties", {}))
        self.opener = build_opener(NoRedirect)
        self.prefix = None
        self._oauth_lock = threading.Lock()
        self._oauth_token = None
        self._oauth_until = 0

    def authorization(self):
        settings = self.config.get("oauth")
        if settings is None:
            return None
        with self._oauth_lock:
            if self._oauth_token and time.monotonic() < self._oauth_until:
                return "Bearer " + self._oauth_token
            started = time.monotonic()
            fields = {
                "grant_type": "client_credentials",
                "client_id": settings["client_id"],
                "client_secret": settings["client_secret"],
                "scope": settings.get("scope", "PRINCIPAL_ROLE:ALL"),
            }
            request = Request(
                settings["uri"],
                urlencode(fields).encode(),
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    **self.config.get("oauth_headers", {}),
                },
                method="POST",
            )
            with self.opener.open(request, timeout=20) as response:
                raw = response.read(65537)
                if len(raw) > 65536:
                    raise Unavailable("OAuth response exceeds budget")
                token = json.loads(raw)
            lifetime = float(token.get("expires_in", 60))
            if token.get("token_type", "bearer").lower() != "bearer" or lifetime <= 0:
                raise Unavailable("invalid OAuth token lifetime/type")
            self._oauth_token = token["access_token"]
            self._oauth_until = started + max(0, lifetime - min(30, lifetime / 2))
            return "Bearer " + self._oauth_token

    def request(self, method, path, body=None, *, native=False):
        base = self.config["nessie_uri" if native else "upstream_uri"].rstrip("/")
        if not path.startswith("/") or path.startswith("//"):
            raise ValueError("invalid provider path")
        headers = dict(self.config.get("upstream_headers", {}))
        if authorization := self.authorization():
            headers["Authorization"] = authorization
        headers.update(
            {"Accept": "application/json", "Content-Type": "application/json"}
        )
        request = Request(base + path, body, headers, method=method)
        try:
            response = self.opener.open(request, timeout=20)
        except HTTPError as error:
            response = error
        with response:
            data = response.read(self.store.max_bytes + 1)
            if len(data) > self.store.max_bytes:
                raise Unavailable("provider response exceeds byte budget")
            return response.status, data

    def json(self, method, path, body=None, *, native=False):
        status, data = self.request(
            method, path, encode(body) if body is not None else None, native=native
        )
        if status == 409:
            raise Conflict("provider catalog conflict")
        if status < 200 or status >= 300:
            raise Unavailable(f"provider returned HTTP {status}")
        return json.loads(data) if data else {}

    def negotiate(self):
        if self.prefix is None:
            query = (
                urlencode({"warehouse": self.config["warehouse"]})
                if self.config.get("warehouse")
                else ""
            )
            result = self.json("GET", "/v1/config" + ("?" + query if query else ""))
            self.prefix = result.get("overrides", {}).get(
                "prefix", result.get("defaults", {}).get("prefix", "")
            )
        return "/v1" + (
            "/" + quote(unquote(self.prefix), safe="") if self.prefix else ""
        )

    def table_path(self, namespace, name):
        return (
            self.negotiate()
            + "/namespaces/"
            + quote("\x1f".join(namespace), safe="")
            + "/tables/"
            + quote(name, safe="")
        )

    def load(self, namespace, name):
        return self.json("GET", self.table_path(namespace, name))

    def commit_expiration(self, namespace, name, current, removed, operation):
        if not removed:
            return current
        metadata = current["metadata"]
        updates = [
            RemoveSnapshotsUpdate(snapshot_ids=removed).model_dump(by_alias=True),
            SetPropertiesUpdate(
                updates={"antfly.maintenance.job": operation}
            ).model_dump(by_alias=True),
        ]
        requirements = [{"type": "assert-table-uuid", "uuid": metadata["table-uuid"]}]
        for name_ref, ref in metadata.get("refs", {}).items():
            requirements.append(
                {
                    "type": "assert-ref-snapshot-id",
                    "ref": name_ref,
                    "snapshot-id": ref["snapshot-id"],
                }
            )
        return self.json(
            "POST",
            self.table_path(namespace, name),
            {"requirements": requirements, "updates": updates},
        )

    def all_metadata(self, *, history_floor_ms=None):
        if self.config["provider"] == "nessie":
            # NONE cutoff: preserve every historical content state on every
            # branch and tag. A branch's REST table view hides these roots.
            refs = self._pages("/trees", "references", native=True)
            for ref in refs:
                reference = quote(ref["name"] + "@" + ref["hash"], safe="")
                if history_floor_ms is not None:
                    # An unchanged table's last Put can predate the cutoff.
                    # Protect every branch/tag's complete head contents first.
                    for entry in self._pages(
                        f"/trees/{reference}/entries?content=true",
                        "entries",
                        native=True,
                    ):
                        content = entry.get("content") or {}
                        if content.get("type") == "ICEBERG_TABLE":
                            yield content["metadataLocation"]
                        elif content.get("type") != "NAMESPACE":
                            raise Unavailable("unsupported Nessie head content")
                for entry in self._pages(
                    f"/trees/{reference}/history?fetch=ALL", "logEntries", native=True
                ):
                    if history_floor_ms is not None:
                        from datetime import datetime

                        timestamp = entry.get("commitMeta", {}).get("commitTime")
                        if timestamp is None:
                            raise Unavailable(
                                "Nessie commit lacks a retention timestamp"
                            )
                        instant = datetime.fromisoformat(
                            timestamp.replace("Z", "+00:00")
                        )
                        if instant.tzinfo is None:
                            raise Unavailable("Nessie timestamp lacks timezone")
                        # Commit clocks need not be strictly ordered: continue
                        # scanning rather than assuming an early stop is safe.
                        if int(instant.timestamp() * 1000) < history_floor_ms:
                            continue
                    for operation in entry.get("operations", []):
                        content = operation.get("content", {})
                        if content.get("type") == "ICEBERG_TABLE":
                            yield content["metadataLocation"]
                        elif content.get("type") not in (None, "NAMESPACE"):
                            raise Unavailable(
                                "provider contains unsupported content roots"
                            )
            return
        todo, visited = [()], set()
        while todo:
            parent = todo.pop()
            if parent in visited:
                continue
            visited.add(parent)
            if len(visited) > self.config.get("max_roots", 100_000):
                raise Unavailable("namespace traversal budget exceeded")
            query = "?" + urlencode({"parent": "\x1f".join(parent)}) if parent else ""
            for namespace in self._pages(
                self.negotiate() + "/namespaces" + query, "namespaces"
            ):
                namespace = tuple(namespace)
                if namespace == parent:
                    continue
                todo.append(namespace)
                path = (
                    self.negotiate()
                    + "/namespaces/"
                    + quote("\x1f".join(namespace), safe="")
                    + "/tables"
                )
                for identifier in self._pages(path, "identifiers"):
                    yield self.load(identifier["namespace"], identifier["name"])[
                        "metadata-location"
                    ]

    def _pages(self, path, field, *, native=False):
        seen = set()
        count = 0
        while True:
            result = self.json("GET", path, native=native)
            for item in result.get(field, []):
                count += 1
                if count > self.config.get("max_roots", 100_000):
                    raise Unavailable("provider root budget exceeded")
                yield item
            token = result.get("token") if native else result.get("next-page-token")
            if not token:
                if result.get("hasMore"):
                    raise Unavailable("provider omitted pagination token")
                return
            if token in seen:
                raise Unavailable("provider repeated pagination token")
            seen.add(token)
            # Rebuild from the original path, without accumulating page tokens.
            parsed = urlsplit(path)
            from urllib.parse import parse_qsl

            params = dict(parse_qsl(parsed.query))
            params["page-token"] = token
            path = parsed.path + "?" + urlencode(params)


class Reachability:
    def __init__(
        self,
        provider,
        warehouse,
        *,
        max_files=100_000,
        max_bytes=256 * 1024 * 1024,
        retirement_check=None,
    ):
        self.provider = provider
        self.retirement_check = retirement_check
        self.warehouse = warehouse.rstrip("/") + "/"
        self.max_files, self.max_bytes = max_files, max_bytes
        self.files = set()
        self.metadata = {}
        self.snapshots = {}
        self.live_snapshots = set()
        self.marked_manifests = set()
        self.bytes = 0

    def check_uri(self, uri):
        parsed = urlsplit(uri)
        if (
            not uri.startswith(self.warehouse)
            or parsed.query
            or parsed.fragment
            or "%" in parsed.path
            or any(part in (".", "..") for part in parsed.path.split("/"))
        ):
            raise Unavailable(
                "metadata references a URI outside the controlled warehouse"
            )
        if self.retirement_check:
            self.retirement_check([uri])
        return uri

    def add(self, uri):
        self.check_uri(uri)
        self.files.add(uri)
        if len(self.files) > self.max_files:
            raise Unavailable("reachability file budget exceeded")

    def read_metadata(self, uri):
        self.check_uri(uri)
        if uri not in self.metadata:
            result = self.provider.store.get(uri)
            if result is None:
                raise Unavailable("catalog root metadata is missing")
            self.bytes += len(result[0])
            if self.bytes > self.max_bytes:
                raise Unavailable("reachability metadata byte budget exceeded")
            self.metadata[uri] = TableMetadataUtil.parse_raw(result[0]).model_dump(
                mode="json", by_alias=True, exclude_none=True
            )
        return self.metadata[uri]

    def mark(self, uri, *, snapshots=None):
        metadata = self.read_metadata(uri)
        self.add(uri)
        self.mark_values(metadata, snapshots=snapshots, root=uri)
        return metadata

    def mark_values(self, metadata, *, snapshots=None, root=None):
        for entry in metadata.get("metadata-log", []):
            self.add(entry["metadata-file"])
        for key in ("statistics", "partition-statistics"):
            for entry in metadata.get(key, []):
                self.add(entry["statistics-path"])
        for snapshot in metadata.get("snapshots", []):
            identifier = str(snapshot["snapshot-id"])
            if root is not None:
                self.snapshots.setdefault(identifier, []).append((root, snapshot))
            if snapshots is None or identifier in snapshots:
                self.mark_snapshot(snapshot)

    def mark_snapshot(self, snapshot):
        if snapshot.get("antfly-empty-metadata"):
            self.add(snapshot["antfly-empty-metadata"])
            self.live_snapshots.add(snapshot["manifest-list"])
            return
        self.live_snapshots.add(snapshot["manifest-list"])
        uri = snapshot.get("manifest-list")
        if not uri:
            raise Unavailable("snapshot lacks a manifest list")
        self.add(uri)
        source = self.manifest_input(uri)
        from pyiceberg.avro.file import AvroFile
        from pyiceberg.manifest import (
            MANIFEST_ENTRY_SCHEMAS,
            DEFAULT_READ_VERSION,
            ManifestEntry,
            DataFile,
            ManifestEntryStatus,
            FileFormat,
            DataFileContent,
        )

        for manifest in read_manifest_list(source):
            self.add(manifest.manifest_path)
            if manifest.manifest_path in self.marked_manifests:
                continue
            source = self.manifest_input(manifest.manifest_path)
            with AvroFile(
                source,
                MANIFEST_ENTRY_SCHEMAS[DEFAULT_READ_VERSION],
                read_types={-1: ManifestEntry, 2: DataFile},
                read_enums={
                    0: ManifestEntryStatus,
                    101: FileFormat,
                    134: DataFileContent,
                },
            ) as reader:
                for entry in reader:
                    if entry.status != ManifestEntryStatus.DELETED:
                        self.add(entry.data_file.file_path)
            # Only completed traversal is reusable. A budget/cancellation or
            # missing object must never publish a partial manifest as marked.
            self.marked_manifests.add(manifest.manifest_path)

    def manifest_input(self, uri):
        source = self.provider.io.new_input(uri)
        self.bytes += len(source)
        if self.bytes > self.max_bytes:
            raise Unavailable("manifest traversal byte budget exceeded")
        return source

    def history(self, uri):
        """Index snapshot IDs in bounded metadata history for native reader pins."""
        todo, visited = [uri], set()
        while todo:
            root = todo.pop()
            if root in visited:
                continue
            visited.add(root)
            if len(visited) > self.max_files:
                raise Unavailable("metadata history budget exceeded")
            metadata = self.read_metadata(root)
            if (
                not metadata.get("snapshots")
                and metadata.get("current-snapshot-id", -1) == -1
            ):
                raw = self.provider.store.get(root)
                if raw is None:
                    raise Unavailable("empty snapshot metadata disappeared")
                identifier = "empty:" + metadata["table-uuid"] + ":" + digest(raw[0])
                self.snapshots.setdefault(identifier, []).append(
                    (
                        root,
                        {
                            "antfly-empty-metadata": root,
                            "manifest-list": "empty-metadata:" + root,
                        },
                    )
                )
            for snapshot in metadata.get("snapshots", []):
                self.snapshots.setdefault(str(snapshot["snapshot-id"]), []).append(
                    (root, snapshot)
                )
            todo.extend(
                entry["metadata-file"] for entry in metadata.get("metadata-log", [])
            )
