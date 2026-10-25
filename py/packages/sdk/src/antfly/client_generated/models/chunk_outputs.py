from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.chunk_output_target import ChunkOutputTarget


T = TypeVar("T", bound="ChunkOutputs")


@_attrs_define
class ChunkOutputs:
    """Named independent media outputs. The named boundary model alone determines the shared plan; adding or changing
    another output preserves parent IDs. The compatibility embedding target is named embedding. Smaller output windows
    are identified by the parent ordinal and sub_index.

        Attributes:
            boundary_model (str):
            targets (list[ChunkOutputTarget]):
    """

    boundary_model: str
    targets: list[ChunkOutputTarget]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        boundary_model = self.boundary_model

        targets = []
        for targets_item_data in self.targets:
            targets_item = targets_item_data.to_dict()
            targets.append(targets_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "boundary_model": boundary_model,
                "targets": targets,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.chunk_output_target import ChunkOutputTarget

        d = dict(src_dict)
        boundary_model = d.pop("boundary_model")

        targets = []
        _targets = d.pop("targets")
        for targets_item_data in _targets:
            targets_item = ChunkOutputTarget.from_dict(targets_item_data)

            targets.append(targets_item)

        chunk_outputs = cls(
            boundary_model=boundary_model,
            targets=targets,
        )

        chunk_outputs.additional_properties = d
        return chunk_outputs

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
