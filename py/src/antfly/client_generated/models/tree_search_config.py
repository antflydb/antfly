from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="TreeSearchConfig")


@_attrs_define
class TreeSearchConfig:
    """Configuration for tree search strategy. Tree search navigates hierarchical
    document structures by evaluating summaries at each level.

        Attributes:
            index (str): Name of the graph index to use for tree navigation Example: doc_hierarchy.
            start_nodes (Union[Unset, str]): Starting nodes for tree search:
                - "$roots" - Query for root nodes (nodes with no parents)
                - Comma-separated explicit node IDs
                When omitted and combined with a QueryRequest in a RetrievalQueryRequest,
                the query results are used as start nodes.
                 Example: $roots.
            max_depth (Union[Unset, int]): Maximum depth to traverse in the tree Default: 5.
            beam_width (Union[Unset, int]): Number of branches to explore at each level Default: 3.
    """

    index: str
    start_nodes: Union[Unset, str] = UNSET
    max_depth: Union[Unset, int] = 5
    beam_width: Union[Unset, int] = 3
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        index = self.index

        start_nodes = self.start_nodes

        max_depth = self.max_depth

        beam_width = self.beam_width

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "index": index,
            }
        )
        if start_nodes is not UNSET:
            field_dict["start_nodes"] = start_nodes
        if max_depth is not UNSET:
            field_dict["max_depth"] = max_depth
        if beam_width is not UNSET:
            field_dict["beam_width"] = beam_width

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        index = d.pop("index")

        start_nodes = d.pop("start_nodes", UNSET)

        max_depth = d.pop("max_depth", UNSET)

        beam_width = d.pop("beam_width", UNSET)

        tree_search_config = cls(
            index=index,
            start_nodes=start_nodes,
            max_depth=max_depth,
            beam_width=beam_width,
        )

        tree_search_config.additional_properties = d
        return tree_search_config

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
