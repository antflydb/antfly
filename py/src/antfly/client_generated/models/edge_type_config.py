from collections.abc import Mapping
from typing import Any, TypeVar, Union, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.edge_type_config_topology import EdgeTypeConfigTopology
from ..types import UNSET, Unset

T = TypeVar("T", bound="EdgeTypeConfig")


@_attrs_define
class EdgeTypeConfig:
    """Configuration for a specific edge type

    Attributes:
        name (str): Edge type name (e.g., 'cites', 'similar_to')
        field (Union[Unset, str]): Document field containing target node key(s) for automatic edge creation.
            Supports string (single target) or array of strings (multiple targets).
            When omitted, edges must be provided explicitly via _edges.
        topology (Union[Unset, EdgeTypeConfigTopology]): Topology constraint for this edge type:
            - tree: Single parent per node, no cycles
            - graph: No constraints (default)
             Default: EdgeTypeConfigTopology.GRAPH.
        max_weight (Union[Unset, float]): Maximum allowed edge weight Default: 1.0.
        min_weight (Union[Unset, float]): Minimum allowed edge weight Default: 0.0.
        allow_self_loops (Union[Unset, bool]): Whether to allow edges from a node to itself Default: True.
        required_metadata (Union[Unset, list[str]]): Required metadata fields for this edge type
    """

    name: str
    field: Union[Unset, str] = UNSET
    topology: Union[Unset, EdgeTypeConfigTopology] = EdgeTypeConfigTopology.GRAPH
    max_weight: Union[Unset, float] = 1.0
    min_weight: Union[Unset, float] = 0.0
    allow_self_loops: Union[Unset, bool] = True
    required_metadata: Union[Unset, list[str]] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        name = self.name

        field = self.field

        topology: Union[Unset, str] = UNSET
        if not isinstance(self.topology, Unset):
            topology = self.topology.value

        max_weight = self.max_weight

        min_weight = self.min_weight

        allow_self_loops = self.allow_self_loops

        required_metadata: Union[Unset, list[str]] = UNSET
        if not isinstance(self.required_metadata, Unset):
            required_metadata = self.required_metadata

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
        if max_weight is not UNSET:
            field_dict["max_weight"] = max_weight
        if min_weight is not UNSET:
            field_dict["min_weight"] = min_weight
        if allow_self_loops is not UNSET:
            field_dict["allow_self_loops"] = allow_self_loops
        if required_metadata is not UNSET:
            field_dict["required_metadata"] = required_metadata

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        name = d.pop("name")

        field = d.pop("field", UNSET)

        _topology = d.pop("topology", UNSET)
        topology: Union[Unset, EdgeTypeConfigTopology]
        if isinstance(_topology, Unset):
            topology = UNSET
        else:
            topology = EdgeTypeConfigTopology(_topology)

        max_weight = d.pop("max_weight", UNSET)

        min_weight = d.pop("min_weight", UNSET)

        allow_self_loops = d.pop("allow_self_loops", UNSET)

        required_metadata = cast(list[str], d.pop("required_metadata", UNSET))

        edge_type_config = cls(
            name=name,
            field=field,
            topology=topology,
            max_weight=max_weight,
            min_weight=min_weight,
            allow_self_loops=allow_self_loops,
            required_metadata=required_metadata,
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
