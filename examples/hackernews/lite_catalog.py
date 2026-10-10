# Copyright 2026 Antfly, Inc.
# SPDX-License-Identifier: Apache-2.0
"""Single-writer PyIceberg catalog backed by the ingestor's Antfly Lite state."""

from pyiceberg.catalog import MetastoreCatalog
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import CommitTableResponse, Table
from pyiceberg.partitioning import UNPARTITIONED_PARTITION_SPEC
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER


class HackernewsCatalog(MetastoreCatalog):
    """Only the example's one table/namespace; not a general catalog service.

    The process owns the writer lock and one Lite handle. Requirements are checked
    against freshly loaded metadata, and the location is committed in a synced
    Lite batch after the immutable metadata object is written.
    """

    def __init__(self, state, warehouse, **properties):
        super().__init__("hn", warehouse=warehouse, **properties)
        self.state = state

    def _identifier(self, identifier):
        value = self.identifier_to_tuple(identifier)
        if value != ("hackernews", "items"):
            raise NoSuchTableError(str(identifier))
        return value

    def load_table(self, identifier):
        identifier = self._identifier(identifier)
        location = self.state.get("iceberg_metadata", "")
        if not location:
            raise NoSuchTableError(str(identifier))
        io = self._load_file_io(location=location)
        metadata = FromInputFile.table_metadata(io.new_input(location))
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
        if self.table_exists(identifier):
            raise TableAlreadyExistsError(str(identifier))
        staged = self._create_staged_table(
            identifier, schema, location, partition_spec, sort_order, properties or {}
        )
        self._write_metadata(staged.metadata, staged.io, staged.metadata_location)
        with self.state.transaction():
            if self.state.get("iceberg_metadata", ""):
                raise TableAlreadyExistsError(str(identifier))
            self.state.set("iceberg_metadata", staged.metadata_location)
        return self.load_table(identifier)

    def commit_table(self, table, requirements, updates):
        identifier = self._identifier(table.name())
        try:
            current = self.load_table(identifier)
        except NoSuchTableError:
            current = None
        staged = self._update_and_stage_table(
            current, identifier, requirements, updates
        )
        if current and staged.metadata == current.metadata:
            return CommitTableResponse(
                metadata=current.metadata, metadata_location=current.metadata_location
            )
        self._write_metadata(staged.metadata, staged.io, staged.metadata_location)
        with self.state.transaction():
            expected = current.metadata_location if current else ""
            if self.state.get("iceberg_metadata", "") != expected:
                raise CommitFailedException("Iceberg writer catalog changed")
            self.state.set("iceberg_metadata", staged.metadata_location)
        return CommitTableResponse(
            metadata=staged.metadata, metadata_location=staged.metadata_location
        )

    def create_namespace(self, namespace, properties=None):
        if self.identifier_to_tuple(namespace) != ("hackernews",):
            raise NoSuchNamespaceError(str(namespace))
        with self.state.transaction():
            if self.state.get("iceberg_namespace", ""):
                raise NamespaceAlreadyExistsError(str(namespace))
            self.state.set("iceberg_namespace", "hackernews")

    def load_namespace_properties(self, namespace):
        if self.identifier_to_tuple(namespace) != ("hackernews",) or not self.state.get(
            "iceberg_namespace", ""
        ):
            raise NoSuchNamespaceError(str(namespace))
        return {}

    def list_tables(self, namespace):
        self.load_namespace_properties(namespace)
        return (
            [("hackernews", "items")] if self.table_exists("hackernews.items") else []
        )

    def list_namespaces(self, namespace=()):
        return (
            [("hackernews",)]
            if not namespace and self.state.get("iceberg_namespace", "")
            else []
        )

    def view_exists(self, identifier):
        return False

    def list_views(self, namespace):
        self.load_namespace_properties(namespace)
        return []

    def _unsupported(self, *args, **kwargs):
        raise NotImplementedError(
            "The HN writer catalog supports one table and no catalog administration"
        )

    register_table = drop_table = purge_table = rename_table = _unsupported
    drop_namespace = update_namespace_properties = _unsupported
    load_view = register_view = drop_view = create_view = _unsupported
