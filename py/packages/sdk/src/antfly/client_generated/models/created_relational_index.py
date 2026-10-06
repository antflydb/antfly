from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.created_relational_index_type import CreatedRelationalIndexType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.created_enrichment_config import CreatedEnrichmentConfig
    from ..models.relational_index_key import RelationalIndexKey
    from ..models.relational_index_predicate import RelationalIndexPredicate


T = TypeVar("T", bound="CreatedRelationalIndex")


@_attrs_define
class CreatedRelationalIndex:
    """Effective schema-bound composite index configuration.

    Attributes:
        name (str): Name of the created index
        keys (list[RelationalIndexKey]):
        type_ (CreatedRelationalIndexType):
        description (str | Unset): Optional description of the index and its purpose
        version (int | Unset): Version of the index implementation. Defaults to 0. Default: 0.
        enrichments (list[CreatedEnrichmentConfig] | Unset): Normalized inline managed enrichment definitions required
            by this index.
        include_columns (list[str] | Unset): Non-key columns stored for index-only projection; distinct from keys.
        where (list[RelationalIndexPredicate] | Unset): Optional conjunction selecting index members. Queries must
            explicitly include all typed conjuncts.
    """

    name: str
    keys: list[RelationalIndexKey]
    type_: CreatedRelationalIndexType
    description: str | Unset = UNSET
    version: int | Unset = 0
    enrichments: list[CreatedEnrichmentConfig] | Unset = UNSET
    include_columns: list[str] | Unset = UNSET
    where: list[RelationalIndexPredicate] | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        keys = []
        for keys_item_data in self.keys:
            keys_item = keys_item_data.to_dict()
            keys.append(keys_item)

        type_ = self.type_.value

        description = self.description

        version = self.version

        enrichments: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.enrichments, Unset):
            enrichments = []
            for enrichments_item_data in self.enrichments:
                enrichments_item = enrichments_item_data.to_dict()
                enrichments.append(enrichments_item)

        include_columns: list[str] | Unset = UNSET
        if not isinstance(self.include_columns, Unset):
            include_columns = self.include_columns

        where: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.where, Unset):
            where = []
            for where_item_data in self.where:
                where_item = where_item_data.to_dict()
                where.append(where_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
                "keys": keys,
                "type": type_,
            }
        )
        if description is not UNSET:
            field_dict["description"] = description
        if version is not UNSET:
            field_dict["version"] = version
        if enrichments is not UNSET:
            field_dict["enrichments"] = enrichments
        if include_columns is not UNSET:
            field_dict["include_columns"] = include_columns
        if where is not UNSET:
            field_dict["where"] = where

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.created_enrichment_config import CreatedEnrichmentConfig
        from ..models.relational_index_key import RelationalIndexKey
        from ..models.relational_index_predicate import RelationalIndexPredicate

        d = dict(src_dict)
        name = d.pop("name")

        keys = []
        _keys = d.pop("keys")
        for keys_item_data in _keys:
            keys_item = RelationalIndexKey.from_dict(keys_item_data)

            keys.append(keys_item)

        type_ = CreatedRelationalIndexType(d.pop("type"))

        description = d.pop("description", UNSET)

        version = d.pop("version", UNSET)

        _enrichments = d.pop("enrichments", UNSET)
        enrichments: list[CreatedEnrichmentConfig] | Unset = UNSET
        if _enrichments is not UNSET:
            enrichments = []
            for enrichments_item_data in _enrichments:
                enrichments_item = CreatedEnrichmentConfig.from_dict(enrichments_item_data)

                enrichments.append(enrichments_item)

        include_columns = cast(list[str], d.pop("include_columns", UNSET))

        _where = d.pop("where", UNSET)
        where: list[RelationalIndexPredicate] | Unset = UNSET
        if _where is not UNSET:
            where = []
            for where_item_data in _where:
                where_item = RelationalIndexPredicate.from_dict(where_item_data)

                where.append(where_item)

        created_relational_index = cls(
            name=name,
            keys=keys,
            type_=type_,
            description=description,
            version=version,
            enrichments=enrichments,
            include_columns=include_columns,
            where=where,
        )

        created_relational_index.additional_properties = d
        return created_relational_index

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
