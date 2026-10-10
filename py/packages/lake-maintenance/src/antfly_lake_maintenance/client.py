# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Lease-aware PyIceberg catalog: file reads fail after authority expires."""

import io
import json
import queue
import weakref
import threading
import time

from pyiceberg.catalog.rest import RestCatalog, Endpoints
from pyiceberg.table import CommitTableRequest, CommitTableResponse, TableIdentifier
from pyiceberg.io import FileIO, InputFile
from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException


class Lease:
    def __init__(self, catalog, value, started=None):
        self.catalog, self.value = catalog, value
        self.deadline = (started if started is not None else time.monotonic()) + 90
        self.closed = False

    def check(self):
        if self.closed or self.catalog.closed or time.monotonic() >= self.deadline:
            raise PermissionError("external reader lease expired")

    def renew(self):
        started = time.monotonic()
        result = self.catalog._session.post(
            self.catalog.gateway_uri + "/v1/antfly/readers/" + self.value["id"],
            json={"ttl_ms": 120_000},
            timeout=20,
        )
        result.raise_for_status()
        self.value = result.json()
        self.deadline = started + 90

    def close(self):
        self.closed = True
        result = self.catalog._session.delete(
            self.catalog.gateway_uri + "/v1/antfly/readers/" + self.value["id"],
            timeout=20,
        )
        result.raise_for_status()


class CheckedStream(io.IOBase):
    def __init__(self, stream, lease):
        self.stream, self.lease = stream, lease

    def read(self, size=-1):
        self.lease.check()
        result = (
            self.stream.read() if size is None or size < 0 else self.stream.read(size)
        )
        self.lease.check()
        return result

    def readinto(self, buffer):
        data = self.read(len(buffer))
        buffer[: len(data)] = data
        return len(data)

    def seek(self, offset, whence=0):
        self.lease.check()
        return self.stream.seek(offset, whence)

    def tell(self):
        return self.stream.tell()

    def readable(self):
        return True

    def seekable(self):
        return True

    def writable(self):
        return False

    def close(self):
        if not self.closed:
            self.stream.close()
        super().close()


class CheckedInput(InputFile):
    def __init__(self, wrapped, lease):
        super().__init__(wrapped.location)
        self.wrapped, self.lease = wrapped, lease

    def __len__(self):
        self.lease.check()
        return len(self.wrapped)

    def exists(self):
        self.lease.check()
        return self.wrapped.exists()

    def open(self, seekable=True):
        self.lease.check()
        return CheckedStream(self.wrapped.open(seekable), self.lease)


class LeasedIO(FileIO):
    def __init__(self, wrapped, lease):
        super().__init__(wrapped.properties)
        self.wrapped, self.lease = wrapped, lease

    def new_input(self, location):
        self.lease.check()
        return CheckedInput(self.wrapped.new_input(location), self.lease)

    def new_output(self, location):
        self.lease.check()
        return self.wrapped.new_output(location)

    def delete(self, location):
        raise PermissionError("physical deletion belongs to the maintenance controller")


class GatewayCatalog(RestCatalog):
    """Use as a context manager; every returned table retains a renewable lease."""

    def __init__(self, name, **properties):
        self.gateway_uri = properties["uri"].removesuffix("/catalog").rstrip("/")
        self.closed = False
        self._leases = weakref.WeakSet()
        self._releases = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._stopping = threading.Event()
        super().__init__(name, **properties)
        self._worker = threading.Thread(target=self._renew, daemon=True)
        self._worker.start()

    def _load_file_io(self, properties=None, location=None):
        from pyiceberg.io.pyarrow import PyArrowFileIO

        # The gateway never transfers vendor catalog credentials or signer
        # endpoints to external clients. Data access uses the caller's own
        # explicitly configured object-store identity.
        return PyArrowFileIO(
            {
                key: value
                for key, value in self.properties.items()
                if key.startswith(("s3.", "gcs."))
            }
        )

    def _attach(self, table, value, started=None):
        lease = Lease(self, value, started)
        with self._lock:
            self._leases.add(lease)
        weakref.finalize(lease, self._releases.put, value["id"])
        base = table.io.wrapped if isinstance(table.io, LeasedIO) else table.io
        table.io = LeasedIO(base, lease)
        return table

    def load_table(self, identifier):
        if self.closed:
            raise PermissionError("catalog is closed")
        identifier = self.identifier_to_tuple(identifier)
        started = time.monotonic()
        response = self._session.post(
            self.gateway_uri + "/v1/antfly/readers",
            json={"namespace": list(identifier[:-1]), "name": identifier[-1]},
            timeout=20,
        )
        response.raise_for_status()
        lease = response.json()
        from pyiceberg.catalog.rest import TableResponse

        result = self._session.get(
            self.url(
                Endpoints.load_table,
                prefixed=True,
                **self._split_identifier_for_path(identifier),
            ),
            headers={**self._session.headers, "X-Antfly-Reader-Lease": lease["id"]},
            timeout=20,
        )
        result.raise_for_status()
        table = super()._response_to_table(
            identifier, TableResponse.model_validate_json(result.text)
        )
        return self._attach(table, lease, started)

    def _response_to_table(self, identifier_tuple, table_response):
        table = super()._response_to_table(identifier_tuple, table_response)
        lease = table_response.config.get("antfly.reader-lease")
        if not lease:
            raise PermissionError("gateway response lacks reader authority")
        return self._attach(table, json.loads(lease))

    def commit_table(self, table, requirements, updates):
        started = time.monotonic()
        identifier = table.name()
        payload = CommitTableRequest(
            identifier=TableIdentifier(namespace=identifier[:-1], name=identifier[-1]),
            requirements=requirements,
            updates=updates,
        )
        response = self._session.post(
            self.url(
                Endpoints.update_table,
                prefixed=True,
                **self._split_identifier_for_path(identifier),
            ),
            data=payload.model_dump_json().encode(),
            timeout=20,
        )
        if response.status_code == 409:
            raise CommitFailedException("gateway rejected the commit")
        if response.status_code >= 500:
            raise CommitStateUnknownException("gateway commit needs reconciliation")
        response.raise_for_status()
        value = response.json()
        if "antfly-reader-lease" not in value:
            raise CommitStateUnknownException("commit response lacks reader authority")
        self._attach(table, value["antfly-reader-lease"], started)
        return CommitTableResponse.model_validate(value)

    def _renew(self):
        while not self._stopping.wait(20):
            for _ in range(1024):
                if self._stopping.is_set():
                    return
                try:
                    identifier = self._releases.get_nowait()
                except queue.Empty:
                    break
                try:
                    self._session.delete(
                        self.gateway_uri + "/v1/antfly/readers/" + identifier,
                        timeout=20,
                    ).raise_for_status()
                except Exception:
                    pass  # An unacknowledged release remains protected until expiry.
            with self._lock:
                leases = list(self._leases)
            for lease in leases:
                if self._stopping.is_set():
                    return
                if lease.closed:
                    continue
                try:
                    lease.renew()
                except Exception:
                    # Failure never extends authority. FileIO checks the
                    # original monotonic deadline before and after each read.
                    pass
            # The worker must not keep the last reader alive between turns.
            if leases:
                del lease
            del leases

    def close(self):
        self.closed = True
        self._stopping.set()
        self._worker.join()
        with self._lock:
            leases = list(self._leases)
        identifiers = {lease.value["id"] for lease in leases}
        for lease in leases:
            lease.closed = True
        while True:
            try:
                identifiers.add(self._releases.get_nowait())
            except queue.Empty:
                break
        deadline = time.monotonic() + 10
        for identifier in identifiers:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                self._session.delete(
                    self.gateway_uri + "/v1/antfly/readers/" + identifier,
                    timeout=min(20, remaining),
                ).raise_for_status()
            except Exception:
                pass  # Expiry still protects an unacknowledged release.
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()
