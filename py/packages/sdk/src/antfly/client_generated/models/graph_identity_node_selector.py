from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.graph_path_endpoint import GraphPathEndpoint


T = TypeVar("T", bound="GraphIdentityNodeSelector")


@_attrs_define
class GraphIdentityNodeSelector:
    """
    Attributes:
        identities (list[GraphPathEndpoint]): Exact node identities. Omitted table means the query table.
    """

    identities: list[GraphPathEndpoint]

    def to_dict(self) -> dict[str, Any]:
        identities = []
        for identities_item_data in self.identities:
            identities_item = identities_item_data.to_dict()
            identities.append(identities_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "identities": identities,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.graph_path_endpoint import GraphPathEndpoint

        d = dict(src_dict)
        identities = []
        _identities = d.pop("identities")
        for identities_item_data in _identities:
            identities_item = GraphPathEndpoint.from_dict(identities_item_data)

            identities.append(identities_item)

        graph_identity_node_selector = cls(
            identities=identities,
        )

        return graph_identity_node_selector
