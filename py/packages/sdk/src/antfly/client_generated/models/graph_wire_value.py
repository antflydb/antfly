from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define

from ..models.graph_wire_value_type import GraphWireValueType

T = TypeVar("T", bound="GraphWireValue")


@_attrs_define
class GraphWireValue:
    """Version 1 graph value. INTEGER payloads are signed decimal strings. Map payloads contain typed values, keeping user
    keys distinct from type tags. Entity identities are opaque table-qualified handles.

        Attributes:
            type_ (GraphWireValueType):
            value (Any): Type-specific payload; nested collections and entity properties contain GraphWireValue values.
    """

    type_: GraphWireValueType
    value: Any

    def to_dict(self) -> dict[str, Any]:
        type_ = self.type_.value

        value = self.value

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "type": type_,
                "value": value,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        type_ = GraphWireValueType(d.pop("type"))

        value = d.pop("value")

        graph_wire_value = cls(
            type_=type_,
            value=value,
        )

        return graph_wire_value
