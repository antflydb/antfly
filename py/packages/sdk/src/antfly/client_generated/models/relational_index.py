from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..models.relational_index_access_method import RelationalIndexAccessMethod
from ..models.relational_index_lifecycle import RelationalIndexLifecycle
from ..models.relational_index_owner_kind import RelationalIndexOwnerKind
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_index_generation_record import RelationalIndexGenerationRecord
    from ..models.relational_index_key import RelationalIndexKey
    from ..models.relational_index_method_config import RelationalIndexMethodConfig
    from ..models.rows_expression_condition import RowsExpressionCondition
    from ..models.rows_unique_predicate_group import RowsUniquePredicateGroup


T = TypeVar("T", bound="RelationalIndex")


@_attrs_define
class RelationalIndex:
    """Durable relational secondary-index metadata.

    Attributes:
        name (str): Stable index name, unique within the table schema.
        owner_kind (RelationalIndexOwnerKind): Catalog object that owns this physical index.
        access_method (RelationalIndexAccessMethod): Logical access method implemented by this index.
        owner_name (str | Unset): Owner column, constraint, or table-level sentinel name.
        method_config (RelationalIndexMethodConfig | Unset): Access-method-specific durable configuration, for example
            full-text analyzer/scoring options or schema-derived algebraic settings.
        unique (bool | Unset): True when the entry backs a unique constraint.
        columns (list[str] | Unset): Declared relational columns maintained by the index.
        include_columns (list[str] | Unset): Covering payload columns stored with ordered tuple entries.
        keys (list[RelationalIndexKey] | Unset): Ordered tuple key definition. Required for ordered_tuple indexes.
        lifecycle (RelationalIndexLifecycle | Unset): Durable lifecycle state for this index generation.
        generation (int | Unset): Monotonic physical index generation.
        schema_fingerprint (str | Unset): Stable fingerprint of the index-defining catalog shape.
        generation_record (RelationalIndexGenerationRecord | Unset): Shared lifecycle record for derived relational
            access-method generations.
        where (RowsUniquePredicateGroup | Unset): Conjunction of partial-unique predicate atoms.
        where_expressions (list[RowsExpressionCondition] | Unset): Deterministic row-expression predicates for index
            participation.
    """

    name: str
    owner_kind: RelationalIndexOwnerKind
    access_method: RelationalIndexAccessMethod
    owner_name: str | Unset = UNSET
    method_config: RelationalIndexMethodConfig | Unset = UNSET
    unique: bool | Unset = UNSET
    columns: list[str] | Unset = UNSET
    include_columns: list[str] | Unset = UNSET
    keys: list[RelationalIndexKey] | Unset = UNSET
    lifecycle: RelationalIndexLifecycle | Unset = UNSET
    generation: int | Unset = UNSET
    schema_fingerprint: str | Unset = UNSET
    generation_record: RelationalIndexGenerationRecord | Unset = UNSET
    where: RowsUniquePredicateGroup | Unset = UNSET
    where_expressions: list[RowsExpressionCondition] | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        owner_kind = self.owner_kind.value

        access_method = self.access_method.value

        owner_name = self.owner_name

        method_config: dict[str, Any] | Unset = UNSET
        if not isinstance(self.method_config, Unset):
            method_config = self.method_config.to_dict()

        unique = self.unique

        columns: list[str] | Unset = UNSET
        if not isinstance(self.columns, Unset):
            columns = self.columns

        include_columns: list[str] | Unset = UNSET
        if not isinstance(self.include_columns, Unset):
            include_columns = self.include_columns

        keys: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.keys, Unset):
            keys = []
            for keys_item_data in self.keys:
                keys_item = keys_item_data.to_dict()
                keys.append(keys_item)

        lifecycle: str | Unset = UNSET
        if not isinstance(self.lifecycle, Unset):
            lifecycle = self.lifecycle.value

        generation = self.generation

        schema_fingerprint = self.schema_fingerprint

        generation_record: dict[str, Any] | Unset = UNSET
        if not isinstance(self.generation_record, Unset):
            generation_record = self.generation_record.to_dict()

        where: dict[str, Any] | Unset = UNSET
        if not isinstance(self.where, Unset):
            where = self.where.to_dict()

        where_expressions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.where_expressions, Unset):
            where_expressions = []
            for where_expressions_item_data in self.where_expressions:
                where_expressions_item = where_expressions_item_data.to_dict()
                where_expressions.append(where_expressions_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "name": name,
                "owner_kind": owner_kind,
                "access_method": access_method,
            }
        )
        if owner_name is not UNSET:
            field_dict["owner_name"] = owner_name
        if method_config is not UNSET:
            field_dict["method_config"] = method_config
        if unique is not UNSET:
            field_dict["unique"] = unique
        if columns is not UNSET:
            field_dict["columns"] = columns
        if include_columns is not UNSET:
            field_dict["include_columns"] = include_columns
        if keys is not UNSET:
            field_dict["keys"] = keys
        if lifecycle is not UNSET:
            field_dict["lifecycle"] = lifecycle
        if generation is not UNSET:
            field_dict["generation"] = generation
        if schema_fingerprint is not UNSET:
            field_dict["schema_fingerprint"] = schema_fingerprint
        if generation_record is not UNSET:
            field_dict["generation_record"] = generation_record
        if where is not UNSET:
            field_dict["where"] = where
        if where_expressions is not UNSET:
            field_dict["where_expressions"] = where_expressions

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_index_generation_record import RelationalIndexGenerationRecord
        from ..models.relational_index_key import RelationalIndexKey
        from ..models.relational_index_method_config import RelationalIndexMethodConfig
        from ..models.rows_expression_condition import RowsExpressionCondition
        from ..models.rows_unique_predicate_group import RowsUniquePredicateGroup

        d = dict(src_dict)
        name = d.pop("name")

        owner_kind = RelationalIndexOwnerKind(d.pop("owner_kind"))

        access_method = RelationalIndexAccessMethod(d.pop("access_method"))

        owner_name = d.pop("owner_name", UNSET)

        _method_config = d.pop("method_config", UNSET)
        method_config: RelationalIndexMethodConfig | Unset
        if isinstance(_method_config, Unset):
            method_config = UNSET
        else:
            method_config = RelationalIndexMethodConfig.from_dict(_method_config)

        unique = d.pop("unique", UNSET)

        columns = cast(list[str], d.pop("columns", UNSET))

        include_columns = cast(list[str], d.pop("include_columns", UNSET))

        _keys = d.pop("keys", UNSET)
        keys: list[RelationalIndexKey] | Unset = UNSET
        if _keys is not UNSET:
            keys = []
            for keys_item_data in _keys:
                keys_item = RelationalIndexKey.from_dict(keys_item_data)

                keys.append(keys_item)

        _lifecycle = d.pop("lifecycle", UNSET)
        lifecycle: RelationalIndexLifecycle | Unset
        if isinstance(_lifecycle, Unset):
            lifecycle = UNSET
        else:
            lifecycle = RelationalIndexLifecycle(_lifecycle)

        generation = d.pop("generation", UNSET)

        schema_fingerprint = d.pop("schema_fingerprint", UNSET)

        _generation_record = d.pop("generation_record", UNSET)
        generation_record: RelationalIndexGenerationRecord | Unset
        if isinstance(_generation_record, Unset):
            generation_record = UNSET
        else:
            generation_record = RelationalIndexGenerationRecord.from_dict(_generation_record)

        _where = d.pop("where", UNSET)
        where: RowsUniquePredicateGroup | Unset
        if isinstance(_where, Unset):
            where = UNSET
        else:
            where = RowsUniquePredicateGroup.from_dict(_where)

        _where_expressions = d.pop("where_expressions", UNSET)
        where_expressions: list[RowsExpressionCondition] | Unset = UNSET
        if _where_expressions is not UNSET:
            where_expressions = []
            for where_expressions_item_data in _where_expressions:
                where_expressions_item = RowsExpressionCondition.from_dict(where_expressions_item_data)

                where_expressions.append(where_expressions_item)

        relational_index = cls(
            name=name,
            owner_kind=owner_kind,
            access_method=access_method,
            owner_name=owner_name,
            method_config=method_config,
            unique=unique,
            columns=columns,
            include_columns=include_columns,
            keys=keys,
            lifecycle=lifecycle,
            generation=generation,
            schema_fingerprint=schema_fingerprint,
            generation_record=generation_record,
            where=where,
            where_expressions=where_expressions,
        )

        return relational_index
