from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.seed_outcome import SeedOutcome


T = TypeVar("T", bound="SeedPlanResponse")


@_attrs_define
class SeedPlanResponse:
    """
    Attributes:
        seeds (list[SeedOutcome]):
    """

    seeds: list[SeedOutcome]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        seeds = []
        for seeds_item_data in self.seeds:
            seeds_item = seeds_item_data.to_dict()
            seeds.append(seeds_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "seeds": seeds,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.seed_outcome import SeedOutcome

        d = dict(src_dict)
        seeds = []
        _seeds = d.pop("seeds")
        for seeds_item_data in _seeds:
            seeds_item = SeedOutcome.from_dict(seeds_item_data)

            seeds.append(seeds_item)

        seed_plan_response = cls(
            seeds=seeds,
        )

        seed_plan_response.additional_properties = d
        return seed_plan_response

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
