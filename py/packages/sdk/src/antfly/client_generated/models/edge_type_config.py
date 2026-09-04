from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.edge_type_config_topology import EdgeTypeConfigTopology
from ..types import UNSET, Unset

T = TypeVar("T", bound="EdgeTypeConfig")


@_attrs_define
class EdgeTypeConfig:
    """Configuration for a specific edge type

    Attributes:
        name (str): Durable graph edge type. Values must be valid UTF-8 and encode to at most 64 KiB; `maxLength` is the
            standard-schema code-point ceiling and `x-antfly-max-utf8-bytes` carries the exact wire-byte limit.
        field (str | Unset): Document field containing target node key(s) for automatic edge creation.
            Supports string (single target) or array of strings (multiple targets).
            When omitted, edges must be provided explicitly via _edges.
        topology (EdgeTypeConfigTopology | Unset): Topology constraint for this edge type:
            - tree: Single parent per node, no cycles
            - graph: No constraints (default)
             Default: EdgeTypeConfigTopology.GRAPH.
    """

    name: str
    field: str | Unset = UNSET
    topology: EdgeTypeConfigTopology | Unset = EdgeTypeConfigTopology.GRAPH
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        field = self.field

        topology: str | Unset = UNSET
        if not isinstance(self.topology, Unset):
            topology = self.topology.value

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "name": name,
            }
        )
        if field is not UNSET:
            field_dict["field"] = field
        if topology is not UNSET:
            field_dict["topology"] = topology

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        field = d.pop("field", UNSET)

        _topology = d.pop("topology", UNSET)
        topology: EdgeTypeConfigTopology | Unset
        if isinstance(_topology, Unset):
            topology = UNSET
        else:
            topology = EdgeTypeConfigTopology(_topology)

        edge_type_config = cls(
            name=name,
            field=field,
            topology=topology,
        )

        edge_type_config.additional_properties = d
        return edge_type_config

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
