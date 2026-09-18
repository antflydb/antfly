from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.table_storage_mode import TableStorageMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.dynamic_template import DynamicTemplate
    from ..models.relational_check_constraint import RelationalCheckConstraint
    from ..models.relational_column_expression import RelationalColumnExpression
    from ..models.relational_foreign_key_constraint import RelationalForeignKeyConstraint
    from ..models.relational_index_definition import RelationalIndexDefinition
    from ..models.relational_unique_constraint import RelationalUniqueConstraint
    from ..models.table_schema_document_schemas import TableSchemaDocumentSchemas
    from ..models.ttl_config import TtlConfig


T = TypeVar("T", bound="TableSchema")


@_attrs_define
class TableSchema:
    """Schema definition for a table with multiple document types

    Attributes:
        version (int | Unset): Backend-managed schema generation used for migrations. Omit it from create and update
            requests.
        storage_mode (TableStorageMode | Unset): Storage representation for the table. Omission selects "document".
            "relational" stores schema-bound typed rows and requires exactly one
            closed document schema with declared properties. It implies
            enforce_types; explicitly setting enforce_types to false is invalid.
            Existing JSON document write and read APIs remain available. This
            setting alone does not declare primary keys or unique constraints.
        column_defaults (list[RelationalColumnExpression] | Unset): Immutable typed expressions applied only to absent
            columns on new
            writes, never explicit null. Defaults cannot reference columns.
            A column cannot have both a default and a generated expression.
            Omission or [] declares none. Relational tables only.
        generated_columns (list[RelationalColumnExpression] | Unset): Stored immutable generated columns, evaluated in
            dependency order
            on writes before validation and indexing. Cycles are rejected.
            Generated columns are output-only; submitted values are replaced
            by the computed value. Omission or [] declares none. Defaults
            and generated declarations together are limited to 256 columns,
            4096 expression nodes, and 4 MiB of literal data. Evaluation has
            a shared 4 MiB allocation budget across all column expressions.
            Restore verifies stored results instead of silently recomputing
            them. Changing, adding, or removing generated semantics through
            an existing table's schema update requires explicit rewrite=true
            on the PUT or PATCH schema route. This returns a durable restore
            job and replaces the complete authorized dependency cohort only
            after distributed transformation and validation. Ordinary schema
            updates reject these changes, even when a table appears empty.
            Declaration reordering and default-only changes remain allowed.
            Relational tables only.
        checks (list[RelationalCheckConstraint] | Unset): Named scalar CHECK constraints for a relational schema. This
            is
            part of the complete schema: omission or [] declares no checks.
            New writes enforce every check. Existing-row validation status is
            maintained separately and is never accepted from the client.
        unique_constraints (list[RelationalUniqueConstraint] | Unset): Complete set of composite unique declarations.
            Omission or [] declares none.
        foreign_keys (list[RelationalForeignKeyConstraint] | Unset): Complete set of outgoing composite foreign keys.
            Omission or [] declares none.
        relational_indexes (list[RelationalIndexDefinition] | Unset): Desired ordered indexes for a relational table.
            Names must be unique.
            An explicit array replaces the declarations; an empty array drops
            them. Omission preserves existing declarations during schema updates.
            Index definitions commit atomically with the schema; build progress
            and readiness are local to each owning shard, not client-writable.
        default_type (str | Unset): Default type to use from the document_types.
        enforce_types (bool | Unset): Whether to enforce that documents must match one of the provided document types.
            If false, documents not matching any type will be accepted but not indexed.
        document_schemas (TableSchemaDocumentSchemas | Unset): A map of type names to their document json schemas.
        ttl (None | TtlConfig | Unset): Automatic document expiration. Set this object to enable TTL and
            set it to null to disable an existing TTL policy.
        ttl_field (str | Unset): Deprecated compatibility alias for `ttl.field`. Cannot be combined with `ttl`.
        ttl_duration (str | Unset): Deprecated compatibility alias for `ttl.duration`. Cannot be combined with `ttl`.
        dynamic_templates (list[DynamicTemplate] | Unset): Rules for mapping dynamically detected fields. When a
            document contains fields
            that don't have explicit mappings and dynamic mapping is enabled, templates are
            evaluated in order to determine how those fields should be indexed.
    """

    version: int | Unset = UNSET
    storage_mode: TableStorageMode | Unset = UNSET
    column_defaults: list[RelationalColumnExpression] | Unset = UNSET
    generated_columns: list[RelationalColumnExpression] | Unset = UNSET
    checks: list[RelationalCheckConstraint] | Unset = UNSET
    unique_constraints: list[RelationalUniqueConstraint] | Unset = UNSET
    foreign_keys: list[RelationalForeignKeyConstraint] | Unset = UNSET
    relational_indexes: list[RelationalIndexDefinition] | Unset = UNSET
    default_type: str | Unset = UNSET
    enforce_types: bool | Unset = UNSET
    document_schemas: TableSchemaDocumentSchemas | Unset = UNSET
    ttl: None | TtlConfig | Unset = UNSET
    ttl_field: str | Unset = UNSET
    ttl_duration: str | Unset = UNSET
    dynamic_templates: list[DynamicTemplate] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        from ..models.ttl_config import TtlConfig

        version = self.version

        storage_mode: str | Unset = UNSET
        if not isinstance(self.storage_mode, Unset):
            storage_mode = self.storage_mode.value

        column_defaults: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.column_defaults, Unset):
            column_defaults = []
            for column_defaults_item_data in self.column_defaults:
                column_defaults_item = column_defaults_item_data.to_dict()
                column_defaults.append(column_defaults_item)

        generated_columns: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.generated_columns, Unset):
            generated_columns = []
            for generated_columns_item_data in self.generated_columns:
                generated_columns_item = generated_columns_item_data.to_dict()
                generated_columns.append(generated_columns_item)

        checks: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.checks, Unset):
            checks = []
            for checks_item_data in self.checks:
                checks_item = checks_item_data.to_dict()
                checks.append(checks_item)

        unique_constraints: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.unique_constraints, Unset):
            unique_constraints = []
            for unique_constraints_item_data in self.unique_constraints:
                unique_constraints_item = unique_constraints_item_data.to_dict()
                unique_constraints.append(unique_constraints_item)

        foreign_keys: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.foreign_keys, Unset):
            foreign_keys = []
            for foreign_keys_item_data in self.foreign_keys:
                foreign_keys_item = foreign_keys_item_data.to_dict()
                foreign_keys.append(foreign_keys_item)

        relational_indexes: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.relational_indexes, Unset):
            relational_indexes = []
            for relational_indexes_item_data in self.relational_indexes:
                relational_indexes_item = relational_indexes_item_data.to_dict()
                relational_indexes.append(relational_indexes_item)

        default_type = self.default_type

        enforce_types = self.enforce_types

        document_schemas: dict[str, Any] | Unset = UNSET
        if not isinstance(self.document_schemas, Unset):
            document_schemas = self.document_schemas.to_dict()

        ttl: dict[str, Any] | None | Unset
        if isinstance(self.ttl, Unset):
            ttl = UNSET
        elif isinstance(self.ttl, TtlConfig):
            ttl = self.ttl.to_dict()
        else:
            ttl = self.ttl

        ttl_field = self.ttl_field

        ttl_duration = self.ttl_duration

        dynamic_templates: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.dynamic_templates, Unset):
            dynamic_templates = []
            for dynamic_templates_item_data in self.dynamic_templates:
                dynamic_templates_item = dynamic_templates_item_data.to_dict()
                dynamic_templates.append(dynamic_templates_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if version is not UNSET:
            field_dict["version"] = version
        if storage_mode is not UNSET:
            field_dict["storage_mode"] = storage_mode
        if column_defaults is not UNSET:
            field_dict["column_defaults"] = column_defaults
        if generated_columns is not UNSET:
            field_dict["generated_columns"] = generated_columns
        if checks is not UNSET:
            field_dict["checks"] = checks
        if unique_constraints is not UNSET:
            field_dict["unique_constraints"] = unique_constraints
        if foreign_keys is not UNSET:
            field_dict["foreign_keys"] = foreign_keys
        if relational_indexes is not UNSET:
            field_dict["relational_indexes"] = relational_indexes
        if default_type is not UNSET:
            field_dict["default_type"] = default_type
        if enforce_types is not UNSET:
            field_dict["enforce_types"] = enforce_types
        if document_schemas is not UNSET:
            field_dict["document_schemas"] = document_schemas
        if ttl is not UNSET:
            field_dict["ttl"] = ttl
        if ttl_field is not UNSET:
            field_dict["ttl_field"] = ttl_field
        if ttl_duration is not UNSET:
            field_dict["ttl_duration"] = ttl_duration
        if dynamic_templates is not UNSET:
            field_dict["dynamic_templates"] = dynamic_templates

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.dynamic_template import DynamicTemplate
        from ..models.relational_check_constraint import RelationalCheckConstraint
        from ..models.relational_column_expression import RelationalColumnExpression
        from ..models.relational_foreign_key_constraint import RelationalForeignKeyConstraint
        from ..models.relational_index_definition import RelationalIndexDefinition
        from ..models.relational_unique_constraint import RelationalUniqueConstraint
        from ..models.table_schema_document_schemas import TableSchemaDocumentSchemas
        from ..models.ttl_config import TtlConfig

        d = dict(src_dict)
        version = d.pop("version", UNSET)

        _storage_mode = d.pop("storage_mode", UNSET)
        storage_mode: TableStorageMode | Unset
        if isinstance(_storage_mode, Unset):
            storage_mode = UNSET
        else:
            storage_mode = TableStorageMode(_storage_mode)

        _column_defaults = d.pop("column_defaults", UNSET)
        column_defaults: list[RelationalColumnExpression] | Unset = UNSET
        if _column_defaults is not UNSET:
            column_defaults = []
            for column_defaults_item_data in _column_defaults:
                column_defaults_item = RelationalColumnExpression.from_dict(column_defaults_item_data)

                column_defaults.append(column_defaults_item)

        _generated_columns = d.pop("generated_columns", UNSET)
        generated_columns: list[RelationalColumnExpression] | Unset = UNSET
        if _generated_columns is not UNSET:
            generated_columns = []
            for generated_columns_item_data in _generated_columns:
                generated_columns_item = RelationalColumnExpression.from_dict(generated_columns_item_data)

                generated_columns.append(generated_columns_item)

        _checks = d.pop("checks", UNSET)
        checks: list[RelationalCheckConstraint] | Unset = UNSET
        if _checks is not UNSET:
            checks = []
            for checks_item_data in _checks:
                checks_item = RelationalCheckConstraint.from_dict(checks_item_data)

                checks.append(checks_item)

        _unique_constraints = d.pop("unique_constraints", UNSET)
        unique_constraints: list[RelationalUniqueConstraint] | Unset = UNSET
        if _unique_constraints is not UNSET:
            unique_constraints = []
            for unique_constraints_item_data in _unique_constraints:
                unique_constraints_item = RelationalUniqueConstraint.from_dict(unique_constraints_item_data)

                unique_constraints.append(unique_constraints_item)

        _foreign_keys = d.pop("foreign_keys", UNSET)
        foreign_keys: list[RelationalForeignKeyConstraint] | Unset = UNSET
        if _foreign_keys is not UNSET:
            foreign_keys = []
            for foreign_keys_item_data in _foreign_keys:
                foreign_keys_item = RelationalForeignKeyConstraint.from_dict(foreign_keys_item_data)

                foreign_keys.append(foreign_keys_item)

        _relational_indexes = d.pop("relational_indexes", UNSET)
        relational_indexes: list[RelationalIndexDefinition] | Unset = UNSET
        if _relational_indexes is not UNSET:
            relational_indexes = []
            for relational_indexes_item_data in _relational_indexes:
                relational_indexes_item = RelationalIndexDefinition.from_dict(relational_indexes_item_data)

                relational_indexes.append(relational_indexes_item)

        default_type = d.pop("default_type", UNSET)

        enforce_types = d.pop("enforce_types", UNSET)

        _document_schemas = d.pop("document_schemas", UNSET)
        document_schemas: TableSchemaDocumentSchemas | Unset
        if isinstance(_document_schemas, Unset):
            document_schemas = UNSET
        else:
            document_schemas = TableSchemaDocumentSchemas.from_dict(_document_schemas)

        def _parse_ttl(data: object) -> None | TtlConfig | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            try:
                if not isinstance(data, dict):
                    raise TypeError()
                ttl_type_1 = TtlConfig.from_dict(data)

                return ttl_type_1
            except (TypeError, ValueError, AttributeError, KeyError):
                pass
            return cast(None | TtlConfig | Unset, data)

        ttl = _parse_ttl(d.pop("ttl", UNSET))

        ttl_field = d.pop("ttl_field", UNSET)

        ttl_duration = d.pop("ttl_duration", UNSET)

        _dynamic_templates = d.pop("dynamic_templates", UNSET)
        dynamic_templates: list[DynamicTemplate] | Unset = UNSET
        if _dynamic_templates is not UNSET:
            dynamic_templates = []
            for dynamic_templates_item_data in _dynamic_templates:
                dynamic_templates_item = DynamicTemplate.from_dict(dynamic_templates_item_data)

                dynamic_templates.append(dynamic_templates_item)

        table_schema = cls(
            version=version,
            storage_mode=storage_mode,
            column_defaults=column_defaults,
            generated_columns=generated_columns,
            checks=checks,
            unique_constraints=unique_constraints,
            foreign_keys=foreign_keys,
            relational_indexes=relational_indexes,
            default_type=default_type,
            enforce_types=enforce_types,
            document_schemas=document_schemas,
            ttl=ttl,
            ttl_field=ttl_field,
            ttl_duration=ttl_duration,
            dynamic_templates=dynamic_templates,
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
