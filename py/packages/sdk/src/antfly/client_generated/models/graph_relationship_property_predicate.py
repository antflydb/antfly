from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_relationship_property_predicate_op import GraphRelationshipPropertyPredicateOp
from ..models.graph_relationship_property_predicate_value_type import GraphRelationshipPropertyPredicateValueType
from ..types import UNSET, Unset

T = TypeVar("T", bound="GraphRelationshipPropertyPredicate")


@_attrs_define
class GraphRelationshipPropertyPredicate:
    """
    Attributes:
        field (str): JSON pointer to /metadata/... or /edge_id, /owner_document, /source, /target, /type, /weight,
            /created_at, /updated_at.
        op (GraphRelationshipPropertyPredicateOp):
        value (Any | Unset): Non-null scalar comparison value. Omit for is_null and is_not_null.
        value_type (GraphRelationshipPropertyPredicateValueType | Unset): Datetime compares RFC3339 instants rather than
            string ordering. Default: GraphRelationshipPropertyPredicateValueType.SCALAR.
    """

    field: str
    op: GraphRelationshipPropertyPredicateOp
    value: Any | Unset = UNSET
    value_type: GraphRelationshipPropertyPredicateValueType | Unset = GraphRelationshipPropertyPredicateValueType.SCALAR

    def to_dict(self) -> dict[str, Any]:
        field = self.field

        op = self.op.value

        value = self.value

        value_type: str | Unset = UNSET
        if not isinstance(self.value_type, Unset):
            value_type = self.value_type.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "field": field,
                "op": op,
            }
        )
        if value is not UNSET:
            field_dict["value"] = value
        if value_type is not UNSET:
            field_dict["value_type"] = value_type

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        field = d.pop("field")

        op = GraphRelationshipPropertyPredicateOp(d.pop("op"))

        value = d.pop("value", UNSET)

        _value_type = d.pop("value_type", UNSET)
        value_type: GraphRelationshipPropertyPredicateValueType | Unset
        if isinstance(_value_type, Unset):
            value_type = UNSET
        else:
            value_type = GraphRelationshipPropertyPredicateValueType(_value_type)

        graph_relationship_property_predicate = cls(
            field=field,
            op=op,
            value=value,
            value_type=value_type,
        )

        return graph_relationship_property_predicate
