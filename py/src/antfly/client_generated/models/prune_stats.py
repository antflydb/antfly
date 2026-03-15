from collections.abc import Mapping
from typing import Any, TypeVar, Union

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..types import UNSET, Unset

T = TypeVar("T", bound="PruneStats")


@_attrs_define
class PruneStats:
    """Statistics from token-based document pruning

    Attributes:
        resources_kept (Union[Unset, int]): Number of resources kept after pruning
        resources_pruned (Union[Unset, int]): Number of resources pruned to fit token budget
        tokens_kept (Union[Unset, int]): Estimated tokens in kept resources
        tokens_pruned (Union[Unset, int]): Estimated tokens in pruned resources
    """

    resources_kept: Union[Unset, int] = UNSET
    resources_pruned: Union[Unset, int] = UNSET
    tokens_kept: Union[Unset, int] = UNSET
    tokens_pruned: Union[Unset, int] = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        resources_kept = self.resources_kept

        resources_pruned = self.resources_pruned

        tokens_kept = self.tokens_kept

        tokens_pruned = self.tokens_pruned

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update({})
        if resources_kept is not UNSET:
            field_dict["resources_kept"] = resources_kept
        if resources_pruned is not UNSET:
            field_dict["resources_pruned"] = resources_pruned
        if tokens_kept is not UNSET:
            field_dict["tokens_kept"] = tokens_kept
        if tokens_pruned is not UNSET:
            field_dict["tokens_pruned"] = tokens_pruned

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        resources_kept = d.pop("resources_kept", UNSET)

        resources_pruned = d.pop("resources_pruned", UNSET)

        tokens_kept = d.pop("tokens_kept", UNSET)

        tokens_pruned = d.pop("tokens_pruned", UNSET)

        prune_stats = cls(
            resources_kept=resources_kept,
            resources_pruned=resources_pruned,
            tokens_kept=tokens_kept,
            tokens_pruned=tokens_pruned,
        )

        prune_stats.additional_properties = d
        return prune_stats

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
