from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.create_relational_index_request_type import CreateRelationalIndexRequestType
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_index_key import RelationalIndexKey
    from ..models.relational_index_predicate import RelationalIndexPredicate


T = TypeVar("T", bound="CreateRelationalIndexRequest")


@_attrs_define
class CreateRelationalIndexRequest:
    """Create a composite ordered index through the shared index resource.

    Attributes:
        keys (list[RelationalIndexKey]):
        type_ (CreateRelationalIndexRequestType):
        include_columns (list[str] | Unset): Non-key columns stored for index-only projection; distinct from keys.
        where (list[RelationalIndexPredicate] | Unset): Optional conjunction selecting index members. Queries must
            explicitly include all typed conjuncts.
        description (str | Unset): Optional description of the index and its purpose.
        version (int | Unset): Index implementation version. Only zero is supported; the schema epoch is managed by the
            server. Default: 0.
    """

    keys: list[RelationalIndexKey]
    type_: CreateRelationalIndexRequestType
    include_columns: list[str] | Unset = UNSET
    where: list[RelationalIndexPredicate] | Unset = UNSET
    description: str | Unset = UNSET
    version: int | Unset = 0
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        keys = []
        for keys_item_data in self.keys:
            keys_item = keys_item_data.to_dict()
            keys.append(keys_item)

        type_ = self.type_.value

        include_columns: list[str] | Unset = UNSET
        if not isinstance(self.include_columns, Unset):
            include_columns = self.include_columns

        where: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.where, Unset):
            where = []
            for where_item_data in self.where:
                where_item = where_item_data.to_dict()
                where.append(where_item)

        description = self.description

        version = self.version

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "keys": keys,
                "type": type_,
            }
        )
        if include_columns is not UNSET:
            field_dict["include_columns"] = include_columns
        if where is not UNSET:
            field_dict["where"] = where
        if description is not UNSET:
            field_dict["description"] = description
        if version is not UNSET:
            field_dict["version"] = version

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_index_key import RelationalIndexKey
        from ..models.relational_index_predicate import RelationalIndexPredicate

        d = dict(src_dict)
        keys = []
        _keys = d.pop("keys")
        for keys_item_data in _keys:
            keys_item = RelationalIndexKey.from_dict(keys_item_data)

            keys.append(keys_item)

        type_ = CreateRelationalIndexRequestType(d.pop("type"))

        include_columns = cast(list[str], d.pop("include_columns", UNSET))

        _where = d.pop("where", UNSET)
        where: list[RelationalIndexPredicate] | Unset = UNSET
        if _where is not UNSET:
            where = []
            for where_item_data in _where:
                where_item = RelationalIndexPredicate.from_dict(where_item_data)

                where.append(where_item)

        description = d.pop("description", UNSET)

        version = d.pop("version", UNSET)

        create_relational_index_request = cls(
            keys=keys,
            type_=type_,
            include_columns=include_columns,
            where=where,
            description=description,
            version=version,
        )

        create_relational_index_request.additional_properties = d
        return create_relational_index_request

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
