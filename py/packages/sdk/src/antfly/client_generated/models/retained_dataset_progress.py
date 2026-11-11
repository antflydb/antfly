from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeVar, cast

from attrs import define as _attrs_define
from attrs import field as _attrs_field

from ..models.retained_dataset_progress_phase import RetainedDatasetProgressPhase
from ..types import UNSET, Unset

if TYPE_CHECKING:
    from ..models.retained_dataset_owner_progress import RetainedDatasetOwnerProgress


T = TypeVar("T", bound="RetainedDatasetProgress")


@_attrs_define
class RetainedDatasetProgress:
    """
    Attributes:
        phase (RetainedDatasetProgressPhase):
        registration_complete (bool):
        registered_owners (int):
        ready_owners (int):
        revoked_owners (int):
        archived_owners (int):
        owners (list[RetainedDatasetOwnerProgress]):
        next_group (None | str | Unset): Pass as after_group to obtain the next bounded owner page.
    """

    phase: RetainedDatasetProgressPhase
    registration_complete: bool
    registered_owners: int
    ready_owners: int
    revoked_owners: int
    archived_owners: int
    owners: list[RetainedDatasetOwnerProgress]
    next_group: None | str | Unset = UNSET
    additional_properties: dict[str, Any] = _attrs_field(init=False, factory=dict)

    def to_dict(self) -> dict[str, Any]:
        phase = self.phase.value

        registration_complete = self.registration_complete

        registered_owners = self.registered_owners

        ready_owners = self.ready_owners

        revoked_owners = self.revoked_owners

        archived_owners = self.archived_owners

        owners = []
        for owners_item_data in self.owners:
            owners_item = owners_item_data.to_dict()
            owners.append(owners_item)

        next_group: None | str | Unset
        if isinstance(self.next_group, Unset):
            next_group = UNSET
        else:
            next_group = self.next_group

        field_dict: dict[str, Any] = {}
        field_dict.update(self.additional_properties)
        field_dict.update(
            {
                "phase": phase,
                "registration_complete": registration_complete,
                "registered_owners": registered_owners,
                "ready_owners": ready_owners,
                "revoked_owners": revoked_owners,
                "archived_owners": archived_owners,
                "owners": owners,
            }
        )
        if next_group is not UNSET:
            field_dict["next_group"] = next_group

        return field_dict

    @classmethod
    def from_dict(cls: type[T], src_dict: Mapping[str, Any]) -> T:
        from ..models.retained_dataset_owner_progress import RetainedDatasetOwnerProgress

        d = dict(src_dict)
        phase = RetainedDatasetProgressPhase(d.pop("phase"))

        registration_complete = d.pop("registration_complete")

        registered_owners = d.pop("registered_owners")

        ready_owners = d.pop("ready_owners")

        revoked_owners = d.pop("revoked_owners")

        archived_owners = d.pop("archived_owners")

        owners = []
        _owners = d.pop("owners")
        for owners_item_data in _owners:
            owners_item = RetainedDatasetOwnerProgress.from_dict(owners_item_data)

            owners.append(owners_item)

        def _parse_next_group(data: object) -> None | str | Unset:
            if data is None:
                return data
            if isinstance(data, Unset):
                return data
            return cast(None | str | Unset, data)

        next_group = _parse_next_group(d.pop("next_group", UNSET))

        retained_dataset_progress = cls(
            phase=phase,
            registration_complete=registration_complete,
            registered_owners=registered_owners,
            ready_owners=ready_owners,
            revoked_owners=revoked_owners,
            archived_owners=archived_owners,
            owners=owners,
            next_group=next_group,
        )

        retained_dataset_progress.additional_properties = d
        return retained_dataset_progress

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
