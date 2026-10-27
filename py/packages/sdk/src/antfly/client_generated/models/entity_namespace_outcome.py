from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

if TYPE_CHECKING:
    from ..models.entity_namespace_outcome_absorbed_item import EntityNamespaceOutcomeAbsorbedItem


T = TypeVar("T", bound="EntityNamespaceOutcome")


@_attrs_define
class EntityNamespaceOutcome:
    """
    Attributes:
        key_template (str):
        revision (int):
        changed (bool):
        absorbed (list[EntityNamespaceOutcomeAbsorbedItem]):
    """

    key_template: str
    revision: int
    changed: bool
    absorbed: list[EntityNamespaceOutcomeAbsorbedItem]
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        key_template = self.key_template

        revision = self.revision

        changed = self.changed

        absorbed = []
        for absorbed_item_data in self.absorbed:
            absorbed_item = absorbed_item_data.to_dict()
            absorbed.append(absorbed_item)

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "key_template": key_template,
                "revision": revision,
                "changed": changed,
                "absorbed": absorbed,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.entity_namespace_outcome_absorbed_item import EntityNamespaceOutcomeAbsorbedItem

        d = dict(src_dict)
        key_template = d.pop("key_template")

        revision = d.pop("revision")

        changed = d.pop("changed")

        absorbed = []
        _absorbed = d.pop("absorbed")
        for absorbed_item_data in _absorbed:
            absorbed_item = EntityNamespaceOutcomeAbsorbedItem.from_dict(absorbed_item_data)

            absorbed.append(absorbed_item)

        entity_namespace_outcome = cls(
            key_template=key_template,
            revision=revision,
            changed=changed,
            absorbed=absorbed,
        )

        entity_namespace_outcome.additional_properties = d
        return entity_namespace_outcome

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
