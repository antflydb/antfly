from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypeVar

from attrs import define as _attrs_define
from attrs import field as _attrs_field

T = TypeVar("T", bound="ManagedSourceDestinationContractOutbound")


@_attrs_define
class ManagedSourceDestinationContractOutbound:
    """
    Attributes:
        document_committed (bool):
        snapshot_then_changes (bool):
        artifact_published (bool):
        publication_activated (bool):
    """

    document_committed: bool
    snapshot_then_changes: bool
    artifact_published: bool
    publication_activated: bool
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        document_committed = self.document_committed

        snapshot_then_changes = self.snapshot_then_changes

        artifact_published = self.artifact_published

        publication_activated = self.publication_activated

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "document_committed": document_committed,
                "snapshot_then_changes": snapshot_then_changes,
                "artifact_published": artifact_published,
                "publication_activated": publication_activated,
            }
        )

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        d = dict(src_dict)
        document_committed = d.pop("document_committed")

        snapshot_then_changes = d.pop("snapshot_then_changes")

        artifact_published = d.pop("artifact_published")

        publication_activated = d.pop("publication_activated")

        managed_source_destination_contract_outbound = cls(
            document_committed=document_committed,
            snapshot_then_changes=snapshot_then_changes,
            artifact_published=artifact_published,
            publication_activated=publication_activated,
        )

        managed_source_destination_contract_outbound.additional_properties = d
        return managed_source_destination_contract_outbound

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
