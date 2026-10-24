from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define

if TYPE_CHECKING:
    from ..models.memory_entity_proposal_inputs_views_type_0_item_dependencies_item import (
        MemoryEntityProposalInputsViewsType0ItemDependenciesItem,
    )


T = TypeVar("T", bound="MemoryEntityProposalInputsViewsType0Item")


@_attrs_define
class MemoryEntityProposalInputsViewsType0Item:
    """
    Attributes:
        id (str):
        canonical_id (None | str):
        fingerprint (str):
        dependencies (list[MemoryEntityProposalInputsViewsType0ItemDependenciesItem]):
    """

    id: str
    canonical_id: None | str
    fingerprint: str
    dependencies: list[MemoryEntityProposalInputsViewsType0ItemDependenciesItem]

    def to_dict(self) -> dict[str, Any]:
        id = self.id

        canonical_id: None | str
        canonical_id = self.canonical_id

        fingerprint = self.fingerprint

        dependencies = []
        for dependencies_item_data in self.dependencies:
            dependencies_item = dependencies_item_data.to_dict()
            dependencies.append(dependencies_item)

        field_dict: dict[str, Any] = {}

        field_dict.update(
            {
                "id": id,
                "canonical_id": canonical_id,
                "fingerprint": fingerprint,
                "dependencies": dependencies,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.memory_entity_proposal_inputs_views_type_0_item_dependencies_item import (
            MemoryEntityProposalInputsViewsType0ItemDependenciesItem,
        )

        d = dict(src_dict)
        id = d.pop("id")

        def _parse_canonical_id(data: object) -> None | str:
            if data is None:
                return data
            return cast(None | str, data)

        canonical_id = _parse_canonical_id(d.pop("canonical_id"))

        fingerprint = d.pop("fingerprint")

        dependencies = []
        _dependencies = d.pop("dependencies")
        for dependencies_item_data in _dependencies:
            dependencies_item = MemoryEntityProposalInputsViewsType0ItemDependenciesItem.from_dict(
                dependencies_item_data
            )

            dependencies.append(dependencies_item)

        memory_entity_proposal_inputs_views_type_0_item = cls(
            id=id,
            canonical_id=canonical_id,
            fingerprint=fingerprint,
            dependencies=dependencies,
        )

        return memory_entity_proposal_inputs_views_type_0_item
