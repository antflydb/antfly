from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.enrichment_config import EnrichmentConfig
    from ..models.field_capability import FieldCapability
    from ..models.replication_source import ReplicationSource
    from ..models.storage_status import StorageStatus
    from ..models.table_indexes import TableIndexes
    from ..models.table_migration import TableMigration
    from ..models.table_schema import TableSchema
    from ..models.table_shards import TableShards


T = TypeVar("T", bound="TableStatus")


@_attrs_define
class TableStatus:
    """
    Attributes:
        name (str):
        indexes (TableIndexes):
        shards (TableShards):
        storage_status (StorageStatus):
        description (str | Unset): Optional description of the table. Example: Table for user data.
        schema (TableSchema | Unset): Schema definition for a table with multiple document types
        migration (TableMigration | Unset): Describes an in-progress schema migration. The table serves reads from
            read_schema while rebuilding full-text indexes for the new schema.
        replication_sources (list[ReplicationSource] | Unset): PostgreSQL CDC replication sources configured for this
            table.
        field_capabilities (list[FieldCapability] | Unset): Effective runtime field capabilities for this table. Clients
            can use this to discover
            concrete field variants and their supported query modes, such as full_text, exact,
            range, geo, and autocomplete. Public exact field sort is supported only for `_id`
            or scalar fields marked sortable whose sort lifecycle is queryable or accelerated.
        artifact_enrichments (list[EnrichmentConfig] | Unset): Table-level generated artifact enrichments registered
            outside a specific index.
    """

    name: str
    indexes: TableIndexes
    shards: TableShards
    storage_status: StorageStatus
    description: str | Unset = UNSET
    schema: TableSchema | Unset = UNSET
    migration: TableMigration | Unset = UNSET
    replication_sources: list[ReplicationSource] | Unset = UNSET
    field_capabilities: list[FieldCapability] | Unset = UNSET
    artifact_enrichments: list[EnrichmentConfig] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        indexes = self.indexes.to_dict()

        shards = self.shards.to_dict()

        storage_status = self.storage_status.to_dict()

        description = self.description

        schema: dict[str, Any] | Unset = UNSET
        if not isinstance(self.schema, Unset):
            schema = self.schema.to_dict()

        migration: dict[str, Any] | Unset = UNSET
        if not isinstance(self.migration, Unset):
            migration = self.migration.to_dict()

        replication_sources: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.replication_sources, Unset):
            replication_sources = []
            for replication_sources_item_data in self.replication_sources:
                replication_sources_item = replication_sources_item_data.to_dict()
                replication_sources.append(replication_sources_item)

        field_capabilities: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.field_capabilities, Unset):
            field_capabilities = []
            for field_capabilities_item_data in self.field_capabilities:
                field_capabilities_item = field_capabilities_item_data.to_dict()
                field_capabilities.append(field_capabilities_item)

        artifact_enrichments: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.artifact_enrichments, Unset):
            artifact_enrichments = []
            for artifact_enrichments_item_data in self.artifact_enrichments:
                artifact_enrichments_item = artifact_enrichments_item_data.to_dict()
                artifact_enrichments.append(artifact_enrichments_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "indexes": indexes,
                "shards": shards,
                "storage_status": storage_status,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if schema is not UNSET:
            field_dict["schema"] = schema
        if migration is not UNSET:
            field_dict["migration"] = migration
        if replication_sources is not UNSET:
            field_dict["replication_sources"] = replication_sources
        if field_capabilities is not UNSET:
            field_dict["field_capabilities"] = field_capabilities
        if artifact_enrichments is not UNSET:
            field_dict["artifact_enrichments"] = artifact_enrichments

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.enrichment_config import EnrichmentConfig
        from ..models.field_capability import FieldCapability
        from ..models.replication_source import ReplicationSource
        from ..models.storage_status import StorageStatus
        from ..models.table_indexes import TableIndexes
        from ..models.table_migration import TableMigration
        from ..models.table_schema import TableSchema
        from ..models.table_shards import TableShards

        d = dict(src_dict)
        name = d.pop("name")

        indexes = TableIndexes.from_dict(d.pop("indexes"))

        shards = TableShards.from_dict(d.pop("shards"))

        storage_status = StorageStatus.from_dict(d.pop("storage_status"))

        description = d.pop("description", UNSET)

        _schema = d.pop("schema", UNSET)
        schema: TableSchema | Unset
        if isinstance(_schema, Unset):
            schema = UNSET
        else:
            schema = TableSchema.from_dict(_schema)

        _migration = d.pop("migration", UNSET)
        migration: TableMigration | Unset
        if isinstance(_migration, Unset):
            migration = UNSET
        else:
            migration = TableMigration.from_dict(_migration)

        _replication_sources = d.pop("replication_sources", UNSET)
        replication_sources: list[ReplicationSource] | Unset = UNSET
        if _replication_sources is not UNSET:
            replication_sources = []
            for replication_sources_item_data in _replication_sources:
                replication_sources_item = ReplicationSource.from_dict(replication_sources_item_data)

                replication_sources.append(replication_sources_item)

        _field_capabilities = d.pop("field_capabilities", UNSET)
        field_capabilities: list[FieldCapability] | Unset = UNSET
        if _field_capabilities is not UNSET:
            field_capabilities = []
            for field_capabilities_item_data in _field_capabilities:
                field_capabilities_item = FieldCapability.from_dict(field_capabilities_item_data)

                field_capabilities.append(field_capabilities_item)

        _artifact_enrichments = d.pop("artifact_enrichments", UNSET)
        artifact_enrichments: list[EnrichmentConfig] | Unset = UNSET
        if _artifact_enrichments is not UNSET:
            artifact_enrichments = []
            for artifact_enrichments_item_data in _artifact_enrichments:
                artifact_enrichments_item = EnrichmentConfig.from_dict(artifact_enrichments_item_data)

                artifact_enrichments.append(artifact_enrichments_item)

        table_status = cls(
            name=name,
            indexes=indexes,
            shards=shards,
            storage_status=storage_status,
            description=description,
            schema=schema,
            migration=migration,
            replication_sources=replication_sources,
            field_capabilities=field_capabilities,
            artifact_enrichments=artifact_enrichments,
        )

        table_status.additional_properties = d
        return table_status

    @property
    def additional_keys(self) -> list[str]:
        return list(self.additional_properties.keys())

    def __getitem__(self, key: str) -> Any:
        return self.additional_properties[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.additional_properties[key] = value

    def __delitem__(self, key: str) -> None:
        del self.additional_properties[key]

    def __contains__(self, key: str) -> bool:
        return key in self.additional_properties
