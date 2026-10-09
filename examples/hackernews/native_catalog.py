# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""PyIceberg file producer committing through Antfly's configured authority."""

import json
import os
import uuid
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from pyiceberg.exceptions import CommitFailedException, NoSuchTableError
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.table.metadata import TableMetadataUtil
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER

from lite_catalog import HackernewsCatalog


class NativeCatalog(HackernewsCatalog):
    """One native table; managed vs REST is selected by its Antfly binding.

    Lite retains the caller request across crashes. Antfly's remote catalog
    journal, rather than this worker state, decides whether a commit succeeded.
    """

    def __init__(self, state, warehouse, endpoint, table_name, **properties):
        super().__init__(state, warehouse, **properties)
        self.endpoint = endpoint.rstrip("/") + "/tables/" + quote(table_name, safe="")

    def _request(self, method, path, body=None):
        headers = {"Accept": "application/json", "Content-Type": "application/json"}
        if token := os.environ.get("ANTFLY_API_KEY"):
            headers["Authorization"] = "Bearer " + token
        payload = None if body is None else json.dumps(body).encode()
        request = Request(self.endpoint + path, payload, headers, method=method)
        with urlopen(request, timeout=60) as response:
            value = json.load(response)
            if response.status == 202:
                raise RuntimeError(
                    "catalog outcome pending; retry the exact saved request"
                )
            return value

    def _finish_pending(self):
        pending = self.state.get("native_catalog_request", "")
        if not pending:
            return None
        intent = json.loads(pending)
        if intent["endpoint"] != self.endpoint:
            raise RuntimeError("pending request belongs to a different native table")
        try:
            result = self._request("POST", intent["path"], intent["body"])
        except HTTPError as error:
            if error.code == 409:
                try:
                    rejection = json.load(error)
                except (ValueError, OSError):
                    rejection = {}
                if rejection.get("error") == "LakeCommitConflict":
                    with self.state.transaction():
                        self.state.set("native_catalog_request", "")
                    raise CommitFailedException(
                        "native catalog rejected the commit"
                    ) from error
            # Auth/config/incarnation failures do not resolve a previously lost
            # response. Retain the request until its original authority resolves it.
            raise
        with self.state.transaction():
            self.state.set("native_catalog_request", "")
        return result

    def _commit(self, path, body):
        # Complete recovery before generating or journaling another request.
        if self.state.get("native_catalog_request", ""):
            raise RuntimeError(
                "load the native table to recover its pending commit first"
            )
        body["commit_id"] = uuid.uuid4().hex
        with self.state.transaction():
            self.state.set(
                "native_catalog_request",
                json.dumps(
                    {
                        "endpoint": self.endpoint,
                        "path": path,
                        "body": body,
                    }
                ),
            )
        return self._finish_pending()

    def load_table(self, identifier):
        identifier = self._identifier(identifier)
        self._finish_pending()
        try:
            result = self._request("GET", "/lake/catalog")
        except HTTPError as error:
            if error.code == 404:
                raise NoSuchTableError(str(identifier)) from error
            raise
        metadata = TableMetadataUtil.parse_obj(result["metadata"])
        location = result["metadata_location"]
        return Table(
            identifier=identifier,
            metadata=metadata,
            metadata_location=location,
            io=self._load_file_io(metadata.properties, location),
            catalog=self,
        )

    def create_table(
        self,
        identifier,
        schema,
        location=None,
        partition_spec=UNPARTITIONED_PARTITION_SPEC,
        sort_order=UNSORTED_SORT_ORDER,
        properties=None,
    ):
        identifier = self._identifier(identifier)
        staged = self._create_staged_table(
            identifier, schema, location, partition_spec, sort_order, properties or {}
        )
        self._commit(
            "/lake/catalog",
            {
                "schema": staged.metadata.schema().model_dump(
                    by_alias=True, mode="json"
                ),
                "partition-spec": staged.metadata.spec().model_dump(
                    by_alias=True, mode="json"
                ),
                "write-order": staged.metadata.sort_order().model_dump(
                    by_alias=True, mode="json"
                ),
                "properties": properties or {},
            },
        )
        return self.load_table(identifier)

    def commit_table(self, table, requirements, updates):
        self._identifier(table.name())
        result = self._commit(
            "/lake/commits",
            {
                "expected_metadata_location": table.metadata_location,
                # snapshot-id:null is meaningful and must survive serialization.
                "requirements": [
                    item.model_dump(by_alias=True, mode="json") for item in requirements
                ],
                "updates": [
                    item.model_dump(by_alias=True, mode="json", exclude_none=True)
                    for item in updates
                ],
            },
        )
        return CommitTableResponse(
            metadata=TableMetadataUtil.parse_obj(result["metadata"]),
            metadata_location=result["metadata_location"],
        )
