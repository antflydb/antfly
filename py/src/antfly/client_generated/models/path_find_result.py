from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.path_find_weight_mode import PathFindWeightMode
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.path import Path


T = TypeVar("T", bound="PathFindResult")


@_attrs_define
class PathFindResult:
    """
    Attributes:
        paths (Union[Unset, list['Path']]):
        source (Union[Unset, str]):
        target (Union[Unset, str]):
        weight_mode (Union[Unset, PathFindWeightMode]): Algorithm for path finding:
            - min_hops: Shortest path by hop count (breadth-first search, ignores weights)
            - max_weight: Path with maximum product of edge weights (strongest connection chain)
            - min_weight: Path with minimum sum of edge weights (lowest cost route)
        paths_found (Union[Unset, int]):
        search_time_ms (Union[Unset, float]):
    """

    paths: Union[Unset, list["Path"]] = UNSET
    source: Union[Unset, str] = UNSET
    target: Union[Unset, str] = UNSET
    weight_mode: Union[Unset, PathFindWeightMode] = UNSET
    paths_found: Union[Unset, int] = UNSET
    search_time_ms: Union[Unset, float] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        paths: Union[Unset, list[dict[str, Any]]] = UNSET
        if not isinstance(self.paths, Unset):
            paths = []
            for paths_item_data in self.paths:
                paths_item = paths_item_data.to_dict()
                paths.append(paths_item)

        source = self.source

        target = self.target

        weight_mode: Union[Unset, str] = UNSET
        if not isinstance(self.weight_mode, Unset):
            weight_mode = self.weight_mode.value

        paths_found = self.paths_found

        search_time_ms = self.search_time_ms

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if paths is not UNSET:
            field_dict["paths"] = paths
        if source is not UNSET:
            field_dict["source"] = source
        if target is not UNSET:
            field_dict["target"] = target
        if weight_mode is not UNSET:
            field_dict["weight_mode"] = weight_mode
        if paths_found is not UNSET:
            field_dict["paths_found"] = paths_found
        if search_time_ms is not UNSET:
            field_dict["search_time_ms"] = search_time_ms

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.path import Path

        d = dict(src_dict)
        paths = []
        _paths = d.pop("paths", UNSET)
        for paths_item_data in _paths or []:
            paths_item = Path.from_dict(paths_item_data)

            paths.append(paths_item)

        source = d.pop("source", UNSET)

        target = d.pop("target", UNSET)

        _weight_mode = d.pop("weight_mode", UNSET)
        weight_mode: Union[Unset, PathFindWeightMode]
        if isinstance(_weight_mode, Unset):
            weight_mode = UNSET
        else:
            weight_mode = PathFindWeightMode(_weight_mode)

        paths_found = d.pop("paths_found", UNSET)

        search_time_ms = d.pop("search_time_ms", UNSET)

        path_find_result = cls(
            paths=paths,
            source=source,
            target=target,
            weight_mode=weight_mode,
            paths_found=paths_found,
            search_time_ms=search_time_ms,
        )

        path_find_result.additional_properties = d
        return path_find_result

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
