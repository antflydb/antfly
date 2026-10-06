from __future__ import annotations

import datetime
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from dateutil.parser import isoparse

from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.graph_relationship_property_predicate import GraphRelationshipPropertyPredicate


T = TypeVar("T", bound="GraphRelationshipFilter")


@_attrs_define
class GraphRelationshipFilter:
    """AND predicates applied to every relationship before neighbor admission, path ranking, and match counting. Missing or
    null properties fail comparisons, including ne; use explicit null operators. Maximum 64 predicates and 64 KiB of
    predicate fields and values. Time intervals have inclusive lower and exclusive upper bounds. Missing/null valid-time
    bounds are open; known_at requires a created_at value. Invalid timestamp properties never match.

        Attributes:
            properties (list[GraphRelationshipPropertyPredicate] | Unset):
            valid_at (datetime.datetime | Unset): Require metadata.valid_at <= instant < metadata.invalid_at.
            known_at (datetime.datetime | Unset): Require metadata.created_at <= instant < metadata.expired_at.
    """

    properties: list[GraphRelationshipPropertyPredicate] | Unset = UNSET
    valid_at: datetime.datetime | Unset = UNSET
    known_at: datetime.datetime | Unset = UNSET

    def to_dict(self) -> dict[str, Any]:
        properties: list[dict[str, Any]] | Unset = UNSET
        if not isinstance(self.properties, Unset):
            properties = []
            for properties_item_data in self.properties:
                properties_item = properties_item_data.to_dict()
                properties.append(properties_item)

        valid_at: str | Unset = UNSET
        if not isinstance(self.valid_at, Unset):
            valid_at = self.valid_at.isoformat()

        known_at: str | Unset = UNSET
        if not isinstance(self.known_at, Unset):
            known_at = self.known_at.isoformat()

        field_dict: dict[str, Any] = {}

        field_dict.update({})
        if properties is not UNSET:
            field_dict["properties"] = properties
        if valid_at is not UNSET:
            field_dict["valid_at"] = valid_at
        if known_at is not UNSET:
            field_dict["known_at"] = known_at

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_relationship_property_predicate import GraphRelationshipPropertyPredicate

        d = dict(src_dict)
        _properties = d.pop("properties", UNSET)
        properties: list[GraphRelationshipPropertyPredicate] | Unset = UNSET
        if _properties is not UNSET:
            properties = []
            for properties_item_data in _properties:
                properties_item = GraphRelationshipPropertyPredicate.from_dict(properties_item_data)

                properties.append(properties_item)

        _valid_at = d.pop("valid_at", UNSET)
        valid_at: datetime.datetime | Unset
        if isinstance(_valid_at, Unset):
            valid_at = UNSET
        else:
            valid_at = isoparse(_valid_at)

        _known_at = d.pop("known_at", UNSET)
        known_at: datetime.datetime | Unset
        if isinstance(_known_at, Unset):
            known_at = UNSET
        else:
            known_at = isoparse(_known_at)

        graph_relationship_filter = cls(
            properties=properties,
            valid_at=valid_at,
            known_at=known_at,
        )

        return graph_relationship_filter
