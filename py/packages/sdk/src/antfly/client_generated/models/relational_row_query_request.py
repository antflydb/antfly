from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.relational_row_condition import RelationalRowCondition


T = TypeVar("T", bound="RelationalRowQueryRequest")


@_attrs_define
class RelationalRowQueryRequest:
    """Bounded relational scan in primary-key order. Each shard read pins an
    immutable schema and row snapshot. Resume with the last returned _id
    as from; a resumed request opens a fresh snapshot, not a retained cursor.
    An empty projection returns row identities and versions only.

        Attributes:
            fields (list[str]):
            conditions (list[RelationalRowCondition] | Unset):
            from_ (str | Unset): Exclusive lower primary-key bound, including pagination continuation.
            to (str | Unset): Exclusive upper primary-key bound.
            limit (int | Unset):  Default: 128.
            schema_version (int | Unset): Reject the read if an owning shard has a different active schema epoch.
    """

    fields: list[str]
    conditions: list[RelationalRowCondition] | Unset = UNSET
    from_: str | Unset = UNSET
    to: str | Unset = UNSET
    limit: int | Unset = 128
    schema_version: int | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        fields = self.fields

        conditions: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.conditions, Unset):
            conditions = []
            for conditions_item_data in self.conditions:
                conditions_item = conditions_item_data.to_dict()
                conditions.append(conditions_item)

        from_ = self.from_

        to = self.to

        limit = self.limit

        schema_version = self.schema_version

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "fields": fields,
            }
        )
        if conditions is not UNSET:
            field_dict["conditions"] = conditions
        if from_ is not UNSET:
            field_dict["from"] = from_
        if to is not UNSET:
            field_dict["to"] = to
        if limit is not UNSET:
            field_dict["limit"] = limit
        if schema_version is not UNSET:
            field_dict["schema_version"] = schema_version

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.relational_row_condition import RelationalRowCondition

        d = dict(src_dict)
        fields = cast(list[str], d.pop("fields"))

        _conditions = d.pop("conditions", UNSET)
        conditions: list[RelationalRowCondition] | Unset = UNSET
        if _conditions is not UNSET:
            conditions = []
            for conditions_item_data in _conditions:
                conditions_item = RelationalRowCondition.from_dict(conditions_item_data)

                conditions.append(conditions_item)

        from_ = d.pop("from", UNSET)

        to = d.pop("to", UNSET)

        limit = d.pop("limit", UNSET)

        schema_version = d.pop("schema_version", UNSET)

        relational_row_query_request = cls(
            fields=fields,
            conditions=conditions,
            from_=from_,
            to=to,
            limit=limit,
            schema_version=schema_version,
        )

        return relational_row_query_request
