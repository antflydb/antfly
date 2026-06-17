from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.table_schema_storage_mode import TableSchemaStorageMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.dynamic_template import DynamicTemplate
    from ..models.foreign_key import ForeignKey
    from ..models.primary_key import PrimaryKey
    from ..models.relational_period import RelationalPeriod
    from ..models.table_schema_document_schemas import TableSchemaDocumentSchemas
    from ..models.unique_constraint import UniqueConstraint


T = TypeVar("T", bound="TableSchema")


@_attrs_define
class TableSchema:
    """Schema definition for a table with multiple document types

    Attributes:
        version (int | Unset): Version of the schema. Used for migrations.
        storage_mode (TableSchemaStorageMode | Unset): Storage profile for the table.
            - "document" (default): schemaless JSON documents with optional,
              soft schema validation. All indexes are derived from the document.
            - "relational": required closed schema with typed columns. Documents
              must match a declared type; declared scalar properties are stored
              as typed columns for columnar predicate pushdown and aggregation.
              A field typed "json" stores a subtree that is still indexed like a
              document. Implies enforce_types and closed document types.
        default_type (str | Unset): Default type to use from the document_types.
        enforce_types (bool | Unset): Whether to enforce that documents must match one of the provided document types.
            If false, documents not matching any type will be accepted but not indexed.
        document_schemas (TableSchemaDocumentSchemas | Unset): A map of type names to their document json schemas.
        ttl_field (str | Unset): The field containing the timestamp for TTL expiration (optional).
            Defaults to "_timestamp" if ttl_duration is specified but ttl_field is not.
        ttl_duration (str | Unset): The duration after which documents should expire, based on the ttl_field timestamp
            (optional).
            Uses Go duration format (e.g., '24h', '7d', '168h').
        dynamic_templates (list[DynamicTemplate] | Unset): Rules for mapping dynamically detected fields. When a
            document contains fields
            that don't have explicit mappings and dynamic mapping is enabled, templates are
            evaluated in order to determine how those fields should be indexed.
        foreign_keys (list[ForeignKey] | Unset): Relational-mode referential constraints. Supported targets are a
            parent table's `_id` document key or a same-table declared unique
            parent column tuple with `on_delete: "restrict"` /
            `on_delete: "no_action"` or bounded local nullable-column
            `on_delete: "set_null"`, plus bounded local `on_delete: "cascade"`.
            `on_update: "restrict"` and `on_update: "no_action"` are accepted
            as parent-key update checks, and mutating `set_null`/`cascade`
            update actions are supported where owner topology is configured.
            Temporal foreign keys with `period` on both child and parent
            references accept restrictive actions plus bounded delete-side
            `set_null` / `cascade` actions through remaining-coverage proofs.
            Mutating temporal update actions are rejected because changing a
            parent interval/key requires update-side period action semantics
            broader than a scalar child-row rewrite.
            `match: "simple"` is the default; `full` is accepted for composite
            nullable references, and `partial` is rejected until row-subset
            parent matching is implemented.
            Cross-table unique targets require routed parent-table unique participants.
            Unsupported shapes are rejected during schema validation.
        periods (list[RelationalPeriod] | Unset): Application-time period declarations over two numeric or datetime
            relational columns. SQL `PERIOD FOR name (start, end)` and range
            column temporal DDL lower into this metadata.
        primary_key (PrimaryKey | Unset): Relational primary-key constraint.
        unique_constraints (list[UniqueConstraint] | Unset): Relational-mode unique constraints over one or more ordered
            declared
            non-json relational columns. Present scalar tuples are enforced by
            committed integrity rows; rows with any absent nullable component do
            not create unique rows.
    """

    version: int | Unset = UNSET
    storage_mode: TableSchemaStorageMode | Unset = UNSET
    default_type: str | Unset = UNSET
    enforce_types: bool | Unset = UNSET
    document_schemas: TableSchemaDocumentSchemas | Unset = UNSET
    ttl_field: str | Unset = UNSET
    ttl_duration: str | Unset = UNSET
    dynamic_templates: list[DynamicTemplate] | Unset = UNSET
    foreign_keys: list[ForeignKey] | Unset = UNSET
    periods: list[RelationalPeriod] | Unset = UNSET
    primary_key: PrimaryKey | Unset = UNSET
    unique_constraints: list[UniqueConstraint] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        version = self.version

        storage_mode: str | Unset = UNSET
        if not isinstance(self.storage_mode, Unset):
            storage_mode = self.storage_mode.value

        default_type = self.default_type

        enforce_types = self.enforce_types

        document_schemas: dict[str, Any] | Unset = UNSET
        if not isinstance(self.document_schemas, Unset):
            document_schemas = self.document_schemas.to_dict()

        ttl_field = self.ttl_field

        ttl_duration = self.ttl_duration

        dynamic_templates: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.dynamic_templates, Unset):
            dynamic_templates = []
            for dynamic_templates_item_data in self.dynamic_templates:
                dynamic_templates_item = dynamic_templates_item_data.to_dict()
                dynamic_templates.append(dynamic_templates_item)

        foreign_keys: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.foreign_keys, Unset):
            foreign_keys = []
            for foreign_keys_item_data in self.foreign_keys:
                foreign_keys_item = foreign_keys_item_data.to_dict()
                foreign_keys.append(foreign_keys_item)

        periods: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.periods, Unset):
            periods = []
            for periods_item_data in self.periods:
                periods_item = periods_item_data.to_dict()
                periods.append(periods_item)

        primary_key: dict[str, Any] | Unset = UNSET
        if not isinstance(self.primary_key, Unset):
            primary_key = self.primary_key.to_dict()

        unique_constraints: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.unique_constraints, Unset):
            unique_constraints = []
            for unique_constraints_item_data in self.unique_constraints:
                unique_constraints_item = unique_constraints_item_data.to_dict()
                unique_constraints.append(unique_constraints_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if version is not UNSET:
            field_dict["version"] = version
        if storage_mode is not UNSET:
            field_dict["storage_mode"] = storage_mode
        if default_type is not UNSET:
            field_dict["default_type"] = default_type
        if enforce_types is not UNSET:
            field_dict["enforce_types"] = enforce_types
        if document_schemas is not UNSET:
            field_dict["document_schemas"] = document_schemas
        if ttl_field is not UNSET:
            field_dict["ttl_field"] = ttl_field
        if ttl_duration is not UNSET:
            field_dict["ttl_duration"] = ttl_duration
        if dynamic_templates is not UNSET:
            field_dict["dynamic_templates"] = dynamic_templates
        if foreign_keys is not UNSET:
            field_dict["foreign_keys"] = foreign_keys
        if periods is not UNSET:
            field_dict["periods"] = periods
        if primary_key is not UNSET:
            field_dict["primary_key"] = primary_key
        if unique_constraints is not UNSET:
            field_dict["unique_constraints"] = unique_constraints

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dynamic_template import DynamicTemplate
        from ..models.foreign_key import ForeignKey
        from ..models.primary_key import PrimaryKey
        from ..models.relational_period import RelationalPeriod
        from ..models.table_schema_document_schemas import TableSchemaDocumentSchemas
        from ..models.unique_constraint import UniqueConstraint

        d = dict(src_dict)
        version = d.pop("version", UNSET)

        _storage_mode = d.pop("storage_mode", UNSET)
        storage_mode: TableSchemaStorageMode | Unset
        if isinstance(_storage_mode, Unset):
            storage_mode = UNSET
        else:
            storage_mode = TableSchemaStorageMode(_storage_mode)

        default_type = d.pop("default_type", UNSET)

        enforce_types = d.pop("enforce_types", UNSET)

        _document_schemas = d.pop("document_schemas", UNSET)
        document_schemas: TableSchemaDocumentSchemas | Unset
        if isinstance(_document_schemas, Unset):
            document_schemas = UNSET
        else:
            document_schemas = TableSchemaDocumentSchemas.from_dict(_document_schemas)

        ttl_field = d.pop("ttl_field", UNSET)

        ttl_duration = d.pop("ttl_duration", UNSET)

        _dynamic_templates = d.pop("dynamic_templates", UNSET)
        dynamic_templates: list[DynamicTemplate] | Unset = UNSET
        if _dynamic_templates is not UNSET:
            dynamic_templates = []
            for dynamic_templates_item_data in _dynamic_templates:
                dynamic_templates_item = DynamicTemplate.from_dict(dynamic_templates_item_data)

                dynamic_templates.append(dynamic_templates_item)

        _foreign_keys = d.pop("foreign_keys", UNSET)
        foreign_keys: list[ForeignKey] | Unset = UNSET
        if _foreign_keys is not UNSET:
            foreign_keys = []
            for foreign_keys_item_data in _foreign_keys:
                foreign_keys_item = ForeignKey.from_dict(foreign_keys_item_data)

                foreign_keys.append(foreign_keys_item)

        _periods = d.pop("periods", UNSET)
        periods: list[RelationalPeriod] | Unset = UNSET
        if _periods is not UNSET:
            periods = []
            for periods_item_data in _periods:
                periods_item = RelationalPeriod.from_dict(periods_item_data)

                periods.append(periods_item)

        _primary_key = d.pop("primary_key", UNSET)
        primary_key: PrimaryKey | Unset
        if isinstance(_primary_key, Unset):
            primary_key = UNSET
        else:
            primary_key = PrimaryKey.from_dict(_primary_key)

        _unique_constraints = d.pop("unique_constraints", UNSET)
        unique_constraints: list[UniqueConstraint] | Unset = UNSET
        if _unique_constraints is not UNSET:
            unique_constraints = []
            for unique_constraints_item_data in _unique_constraints:
                unique_constraints_item = UniqueConstraint.from_dict(unique_constraints_item_data)

                unique_constraints.append(unique_constraints_item)

        table_schema = cls(
            version=version,
            storage_mode=storage_mode,
            default_type=default_type,
            enforce_types=enforce_types,
            document_schemas=document_schemas,
            ttl_field=ttl_field,
            ttl_duration=ttl_duration,
            dynamic_templates=dynamic_templates,
            foreign_keys=foreign_keys,
            periods=periods,
            primary_key=primary_key,
            unique_constraints=unique_constraints,
        )

        table_schema.additional_properties = d
        return table_schema

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
